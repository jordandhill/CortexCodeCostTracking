from datetime import date, timedelta
import json
import os
import re
import tempfile
import urllib.request
import pandas as pd
import streamlit as st
import altair as alt

IS_SIS = False
_snowpark_session = None
try:
    from snowflake.snowpark.context import get_active_session
    _snowpark_session = get_active_session()
    IS_SIS = True
except Exception:
    pass

if not IS_SIS:
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization
    from pathlib import Path
    import tomllib

st.set_page_config(
    page_title="Snowflake AI Spend",
    page_icon=":material/monitoring:",
    layout="wide",
)

TIME_RANGES = ["Last 30 days", "Last 90 days", "Last 6 months", "Last 12 months", "YTD", "All"]
CHART_HEIGHT = 350
SOURCES = {"CLI": "CORTEX_CODE_CLI_USAGE_HISTORY", "Snowsight": "CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY"}

CREDIT_PRICE_TIERS = {
    "Global ($2.00) — effective Apr 1, 2026": 2.00,
    "In-region ($2.20) — effective Apr 1, 2026": 2.20,
}

CORTEX_CODE_PRICING = {
    "claude-4-sonnet": {"input": 1.50, "cache_read_input": 0.15, "cache_write_input": 1.88, "output": 7.50},
    "claude-opus-4-5": {"input": 2.75, "cache_read_input": 0.28, "cache_write_input": 3.44, "output": 13.75},
    "claude-opus-4-6": {"input": 2.75, "cache_read_input": 0.28, "cache_write_input": 3.44, "output": 13.75},
    "claude-sonnet-4-5": {"input": 1.65, "cache_read_input": 0.17, "cache_write_input": 2.07, "output": 8.25},
    "claude-sonnet-4-6": {"input": 1.65, "cache_read_input": 0.17, "cache_write_input": 2.07, "output": 8.25},
    "openai-gpt-5.2": {"input": 0.97, "cache_read_input": 0.10, "cache_write_input": 0.0, "output": 7.70},
    "openai-gpt-5.44": {"input": 1.38, "cache_read_input": 0.14, "cache_write_input": 0.0, "output": 8.25},
}

AI_COMPLETE_PRICING = {
    "claude-3-5-sonnet": {"input": 1.50, "output": 7.50},
    "claude-3-haiku": {"input": 0.21, "output": 1.05},
    "claude-3-opus": {"input": 11.25, "output": 56.25},
    "mistral-large2": {"input": 0.97, "output": 7.70},
    "mixtral-8x7b": {"input": 0.21, "output": 0.21},
    "llama3.1-70b": {"input": 0.28, "output": 0.28},
    "llama3.1-8b": {"input": 0.06, "output": 0.06},
    "snowflake-arctic": {"input": 0.28, "output": 0.28},
}

CORTEX_SEARCH_RATE_PER_1K = 0.25
CORTEX_ANALYST_RATE_PER_REQUEST = 0.06

CONSUMPTION_TABLE_URL = "https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf"
PRICING_STAGE = "CORTEX_CODE_DASHBOARD.PUBLIC.PRICING_DOCS"

TOKEN_TYPE_LABELS = {
    "input": "Input",
    "cache_read": "Cache Read",
    "cache_write": "Cache Write",
    "output": "Output",
}

AI_FUNCTION_TYPES = [
    "AI_COMPLETE", "AI_CLASSIFY", "AI_FILTER", "AI_SUMMARIZE",
    "AI_TRANSLATE", "AI_SENTIMENT", "AI_EXTRACT", "AI_AGG",
]


if not IS_SIS:
    @st.cache_resource
    def get_conn():
        try:
            conn_name = os.getenv("SNOWFLAKE_CONNECTION_NAME") or os.getenv("SNOWFLAKE_DEFAULT_CONNECTION_NAME") or "default"
            toml_path = Path.home() / ".snowflake" / "connections.toml"
            cfg = {}
            if toml_path.exists():
                with open(toml_path, "rb") as f:
                    all_conns = tomllib.load(f)
                cfg = all_conns.get(conn_name, {})
            key_path = cfg.get("private_key_path")
            kwargs = {
                "account": cfg.get("account"),
                "user": cfg.get("user"),
                "role": cfg.get("role"),
                "warehouse": cfg.get("warehouse"),
            }
            if key_path:
                with open(key_path, "rb") as kf:
                    pk = serialization.load_pem_private_key(kf.read(), password=None)
                kwargs["private_key"] = pk.private_bytes(
                    serialization.Encoding.DER,
                    serialization.PrivateFormat.PKCS8,
                    serialization.NoEncryption(),
                )
            elif cfg.get("authenticator"):
                kwargs["authenticator"] = cfg["authenticator"]
            return snowflake.connector.connect(**{k: v for k, v in kwargs.items() if v is not None})
        except Exception as e:
            st.error(f"Failed to connect to Snowflake: {e}")
            st.info("Set SNOWFLAKE_DEFAULT_CONNECTION_NAME or configure a 'default' connection.")
            st.stop()


def run_query(sql: str) -> pd.DataFrame:
    if IS_SIS:
        df = _snowpark_session.sql(sql).to_pandas()
        df.columns = [c.lower() for c in df.columns]
        return df
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        rows = cur.fetchall()
        cols = [desc[0].lower() for desc in cur.description]
        return pd.DataFrame(rows, columns=cols)
    finally:
        cur.close()


def run_scalar(sql: str):
    if IS_SIS:
        return _snowpark_session.sql(sql).collect()[0][0]
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(sql)
        return cur.fetchone()[0]
    finally:
        cur.close()


def _parse_pricing_from_json(raw_json: str) -> dict:
    parsed = json.loads(raw_json)
    pricing = {}
    for page in parsed.get("pages", []):
        content = page.get("content", "")
        if "6(g)" not in content or "cortex code" not in content.lower():
            continue
        for line in content.split("\n"):
            line = line.strip().strip("|")
            if not line or line.startswith("---") or "Model" in line or "Snowflake" in line:
                continue
            parts = [c.strip() for c in line.split("|")]
            if len(parts) < 5:
                continue
            model = parts[0].strip()
            if not model or not re.match(r"^[a-z]", model):
                continue
            try:
                inp = float(parts[1])
                out = float(parts[2])
                cw_raw = parts[3].strip()
                cache_write = float(cw_raw) if cw_raw != "-" else 0.0
                cr = float(parts[4])
                pricing[model] = {
                    "input": inp,
                    "output": out,
                    "cache_write_input": cache_write,
                    "cache_read_input": cr,
                }
            except (ValueError, IndexError):
                continue
        break
    return pricing


def refresh_pricing_from_pdf() -> dict:
    if IS_SIS:
        raise RuntimeError(
            "PDF download from the internet is not available in Streamlit in Snowflake. "
            "Upload the PDF to the stage manually and use 'Refresh from Stage' instead."
        )
    with tempfile.TemporaryDirectory() as td:
        pdf_path = os.path.join(td, "CreditConsumptionTable.pdf")
        req = urllib.request.Request(CONSUMPTION_TABLE_URL, headers={
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
        })
        with urllib.request.urlopen(req) as resp, open(pdf_path, "wb") as out:
            out.write(resp.read())
        conn = get_conn()
        cur = conn.cursor()
        try:
            cur.execute("CREATE DATABASE IF NOT EXISTS CORTEX_CODE_DASHBOARD")
            cur.execute("CREATE SCHEMA IF NOT EXISTS CORTEX_CODE_DASHBOARD.PUBLIC")
            cur.execute(f"CREATE STAGE IF NOT EXISTS {PRICING_STAGE} ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')")
            cur.execute(f"PUT file://{pdf_path} @{PRICING_STAGE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE")
        finally:
            cur.close()
    raw_json = run_scalar(f"""
        SELECT AI_PARSE_DOCUMENT(
            TO_FILE('@{PRICING_STAGE}', 'CreditConsumptionTable.pdf'),
            {{'mode': 'LAYOUT', 'page_split': true}}
        )::VARCHAR
    """)
    return _parse_pricing_from_json(raw_json)


def refresh_pricing_from_stage() -> dict:
    raw_json = run_scalar(f"""
        SELECT AI_PARSE_DOCUMENT(
            TO_FILE('@{PRICING_STAGE}', 'CreditConsumptionTable.pdf'),
            {{'mode': 'LAYOUT', 'page_split': true}}
        )::VARCHAR
    """)
    return _parse_pricing_from_json(raw_json)


def filter_by_time_range(df: pd.DataFrame, x_col: str, time_range: str) -> pd.DataFrame:
    if time_range == "All" or df.empty:
        return df
    df = df.copy()
    df[x_col] = pd.to_datetime(df[x_col])
    max_date = df[x_col].max()
    if time_range == "Last 30 days":
        min_date = max_date - timedelta(days=30)
    elif time_range == "Last 90 days":
        min_date = max_date - timedelta(days=90)
    elif time_range == "Last 6 months":
        min_date = max_date - timedelta(days=180)
    elif time_range == "Last 12 months":
        min_date = max_date - timedelta(days=365)
    elif time_range == "YTD":
        min_date = pd.Timestamp(date(max_date.year, 1, 1))
    else:
        return df
    return df[df[x_col] >= min_date]


@st.cache_data(ttl=600, show_spinner="Loading Cortex Code usage data...")
def load_usage_data() -> pd.DataFrame:
    frames = []
    for label, view in SOURCES.items():
        try:
            df = run_query(f"""
                SELECT
                    u.NAME AS user_name,
                    '{label}' AS source,
                    DATE_TRUNC('day', c.USAGE_TIME)::DATE AS usage_date,
                    c.TOKEN_CREDITS,
                    c.TOKENS,
                    c.REQUEST_ID
                FROM SNOWFLAKE.ACCOUNT_USAGE.{view} c
                LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON c.USER_ID = u.USER_ID
            """)
            df["token_credits"] = df["token_credits"].astype(float)
            df["tokens"] = df["tokens"].astype(float)
            frames.append(df)
        except Exception:
            pass
    if not frames:
        st.error("No Cortex Code usage data found. Ensure you have ACCOUNTADMIN or USAGE_VIEWER access.")
        st.stop()
    return pd.concat(frames, ignore_index=True)


@st.cache_data(ttl=600, show_spinner="Loading Cortex Code model-level data...")
def load_granular_data() -> pd.DataFrame:
    frames = []
    for label, view in SOURCES.items():
        try:
            df = run_query(f"""
                SELECT
                    u.NAME AS user_name,
                    '{label}' AS source,
                    DATE_TRUNC('day', c.USAGE_TIME)::DATE AS usage_date,
                    c.REQUEST_ID,
                    f.key AS model_name,
                    f.value:input::FLOAT AS input_tokens,
                    f.value:cache_read_input::FLOAT AS cache_read_tokens,
                    f.value:cache_write_input::FLOAT AS cache_write_tokens,
                    f.value:output::FLOAT AS output_tokens,
                    g.value:input::FLOAT AS input_credits,
                    g.value:cache_read_input::FLOAT AS cache_read_credits,
                    g.value:cache_write_input::FLOAT AS cache_write_credits,
                    g.value:output::FLOAT AS output_credits
                FROM SNOWFLAKE.ACCOUNT_USAGE.{view} c
                LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON c.USER_ID = u.USER_ID,
                LATERAL FLATTEN(c.TOKENS_GRANULAR) f,
                LATERAL FLATTEN(c.CREDITS_GRANULAR) g
                WHERE f.key = g.key
            """)
            frames.append(df)
        except Exception:
            pass
    if not frames:
        return pd.DataFrame()
    result = pd.concat(frames, ignore_index=True)
    for col in ["input_tokens", "cache_read_tokens", "cache_write_tokens", "output_tokens",
                "input_credits", "cache_read_credits", "cache_write_credits", "output_credits"]:
        result[col] = result[col].fillna(0).astype(float)
    return result


@st.cache_data(ttl=600, show_spinner="Loading AI Functions data...")
def load_ai_functions_data() -> pd.DataFrame:
    try:
        df = run_query("""
            SELECT
                DATE_TRUNC('day', f.START_TIME)::DATE AS usage_date,
                COALESCE(f.FUNCTION_NAME, 'Unknown') AS function_name,
                COALESCE(f.MODEL_NAME, 'N/A') AS model_name,
                COALESCE(u.NAME, 'Unknown') AS user_name,
                SUM(f.CREDITS) AS credits,
                COUNT(*) AS calls
            FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY f
            LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON f.USER_ID = u.USER_ID
            GROUP BY 1, 2, 3, 4
        """)
        df["credits"] = df["credits"].astype(float)
        df["calls"] = df["calls"].astype(int)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner="Loading Cortex Search data...")
def load_cortex_search_data() -> pd.DataFrame:
    try:
        df = run_query("""
            SELECT
                DATE_TRUNC('day', START_TIME)::DATE AS usage_date,
                DATABASE_NAME AS database_name,
                SCHEMA_NAME AS schema_name,
                SERVICE_NAME AS service_name,
                SUM(CREDITS) AS credits
            FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_SERVING_USAGE_HISTORY
            GROUP BY 1, 2, 3, 4
        """)
        df["credits"] = df["credits"].astype(float)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner="Loading Cortex Analyst data...")
def load_cortex_analyst_data() -> pd.DataFrame:
    try:
        df = run_query("""
            SELECT
                DATE_TRUNC('day', START_TIME)::DATE AS usage_date,
                COALESCE(USERNAME, 'Unknown') AS user_name,
                SUM(CREDITS) AS credits,
                SUM(REQUEST_COUNT) AS requests
            FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY
            GROUP BY 1, 2
        """)
        df["credits"] = df["credits"].astype(float)
        df["requests"] = df["requests"].astype(float)
        return df
    except Exception:
        return pd.DataFrame()


@st.cache_data(ttl=600, show_spinner="Loading Agents & Intelligence data...")
def load_agents_data() -> pd.DataFrame:
    try:
        df = run_query("""
            SELECT
                DATE_TRUNC('day', START_TIME)::DATE AS usage_date,
                'Cortex Agent' AS product,
                COALESCE(USER_NAME, 'Unknown') AS user_name,
                COALESCE(AGENT_NAME, 'Unknown') AS resource_name,
                SUM(TOKEN_CREDITS) AS credits,
                SUM(TOKENS) AS tokens
            FROM SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AGENT_USAGE_HISTORY
            GROUP BY 1, 2, 3, 4
            UNION ALL
            SELECT
                DATE_TRUNC('day', START_TIME)::DATE AS usage_date,
                'Snowflake Intelligence' AS product,
                COALESCE(USER_NAME, 'Unknown') AS user_name,
                COALESCE(SNOWFLAKE_INTELLIGENCE_NAME, 'Unknown') AS resource_name,
                SUM(TOKEN_CREDITS) AS credits,
                SUM(TOKENS) AS tokens
            FROM SNOWFLAKE.ACCOUNT_USAGE.SNOWFLAKE_INTELLIGENCE_USAGE_HISTORY
            GROUP BY 1, 2, 3, 4
        """)
        df["credits"] = df["credits"].astype(float)
        df["tokens"] = df["tokens"].astype(float)
        return df
    except Exception:
        return pd.DataFrame()


if not IS_SIS:
    get_conn()

hdr_left, hdr_spacer, hdr_right = st.columns([6, 2, 2])
with hdr_left:
    st.title("Snowflake AI Spend")
with hdr_right:
    time_range = st.selectbox("Date range", TIME_RANGES, index=0, key="tr", label_visibility="collapsed")

raw = load_usage_data()
granular = load_granular_data()
available_sources = sorted(raw["source"].unique())

with st.sidebar:
    st.header("Settings")
    tier_labels = list(CREDIT_PRICE_TIERS.keys())
    selected_tier = st.selectbox("AI credit pricing tier", tier_labels, index=0, key="tier")
    price_per_credit = CREDIT_PRICE_TIERS[selected_tier]
    customer_credit_price = st.number_input(
        "Customer cost per credit ($)",
        min_value=0.01, max_value=50.00, value=3.00, step=0.01, format="%.2f",
        key="cust_cpc",
        help="Your negotiated credit price. Used for products billed on traditional credits (e.g. AI Functions).",
    )
    st.caption(f"AI credit price: **${price_per_credit:.2f}** · Customer credit price: **${customer_credit_price:.2f}**")
    st.divider()
    st.subheader("Cortex Code filters")
    source_filter = st.multiselect("Source", available_sources, default=available_sources, key="src")
    all_users = sorted(raw["user_name"].dropna().unique())
    user_filter = st.multiselect("Users", all_users, default=all_users, key="users")

source_filter = source_filter or available_sources
user_filter = user_filter or all_users

filtered = raw[raw["source"].isin(source_filter) & raw["user_name"].isin(user_filter)]
filtered = filter_by_time_range(filtered, "usage_date", time_range)

if not granular.empty:
    granular_filtered = granular[granular["source"].isin(source_filter) & granular["user_name"].isin(user_filter)]
    granular_filtered = filter_by_time_range(granular_filtered, "usage_date", time_range)
else:
    granular_filtered = pd.DataFrame()

if filtered.empty:
    st.warning("No Cortex Code data matches the current filters.")

total_credits_code = filtered["token_credits"].sum() if not filtered.empty else 0.0
total_cost_code = total_credits_code * price_per_credit
total_cost_aifn = 0.0
total_requests_code = filtered["request_id"].nunique() if not filtered.empty else 0
total_tokens_code = filtered["tokens"].sum() if not filtered.empty else 0.0
active_users_code = filtered["user_name"].nunique() if not filtered.empty else 0

daily_code = pd.DataFrame()
if not filtered.empty:
    daily_code = (
        filtered.groupby(["usage_date", "source"])
        .agg(credits=("token_credits", "sum"), tokens=("tokens", "sum"), requests=("request_id", "nunique"))
        .reset_index()
    )
    daily_code["cost"] = daily_code["credits"] * price_per_credit
    daily_code["usage_date"] = pd.to_datetime(daily_code["usage_date"])

model_summary = pd.DataFrame()
if not granular_filtered.empty:
    model_summary = (
        granular_filtered.groupby("model_name")
        .agg(
            input_tokens=("input_tokens", "sum"),
            cache_read_tokens=("cache_read_tokens", "sum"),
            cache_write_tokens=("cache_write_tokens", "sum"),
            output_tokens=("output_tokens", "sum"),
            input_credits=("input_credits", "sum"),
            cache_read_credits=("cache_read_credits", "sum"),
            cache_write_credits=("cache_write_credits", "sum"),
            output_credits=("output_credits", "sum"),
            requests=("request_id", "nunique"),
        )
        .reset_index()
    )
    model_summary["total_tokens"] = (
        model_summary["input_tokens"] + model_summary["cache_read_tokens"]
        + model_summary["cache_write_tokens"] + model_summary["output_tokens"]
    )
    model_summary["total_credits"] = (
        model_summary["input_credits"] + model_summary["cache_read_credits"]
        + model_summary["cache_write_credits"] + model_summary["output_credits"]
    )
    model_summary["total_cost"] = model_summary["total_credits"] * price_per_credit

aifn_raw = load_ai_functions_data()
search_raw = load_cortex_search_data()
analyst_raw = load_cortex_analyst_data()
agents_raw = load_agents_data()

aifn_filtered = filter_by_time_range(aifn_raw, "usage_date", time_range) if not aifn_raw.empty else pd.DataFrame()
search_filtered = filter_by_time_range(search_raw, "usage_date", time_range) if not search_raw.empty else pd.DataFrame()
analyst_filtered = filter_by_time_range(analyst_raw, "usage_date", time_range) if not analyst_raw.empty else pd.DataFrame()
agents_filtered = filter_by_time_range(agents_raw, "usage_date", time_range) if not agents_raw.empty else pd.DataFrame()

total_credits_aifn = aifn_filtered["credits"].sum() if not aifn_filtered.empty else 0.0
total_cost_aifn = total_credits_aifn * customer_credit_price
total_credits_search = search_filtered["credits"].sum() if not search_filtered.empty else 0.0
total_credits_analyst = analyst_filtered["credits"].sum() if not analyst_filtered.empty else 0.0
total_credits_agents = agents_filtered["credits"].sum() if not agents_filtered.empty else 0.0


(tab_overview, tab_code, tab_aifn, tab_search, tab_analyst, tab_agents,
 tab_estimator, tab_pricing) = st.tabs([
    ":material/dashboard: Overview",
    ":material/code: Cortex Code",
    ":material/functions: AI Functions",
    ":material/search: Cortex Search",
    ":material/analytics: Cortex Analyst",
    ":material/smart_toy: Agents & SI",
    ":material/calculate: Cost Estimator",
    ":material/price_check: Pricing Reference",
])


@st.fragment
def render_overview():
    st.subheader("AI spend by product")

    ov1, ov2, ov3, ov4, ov5 = st.columns(5)
    ov1.metric("Cortex Code", f"{total_credits_code:,.1f} cr", f"${total_cost_code:,.2f}")
    ov2.metric("AI Functions", f"{total_credits_aifn:,.1f} cr", f"${total_cost_aifn:,.2f} (credit-based)")
    ov3.metric("Cortex Search", f"{total_credits_search:,.1f} cr", f"${total_credits_search * price_per_credit:,.2f}")
    ov4.metric("Cortex Analyst", f"{total_credits_analyst:,.1f} cr", f"${total_credits_analyst * price_per_credit:,.2f}")
    ov5.metric("Agents & SI", f"{total_credits_agents:,.1f} cr", f"${total_credits_agents * price_per_credit:,.2f}")

    total_all_credits = total_credits_code + total_credits_aifn + total_credits_search + total_credits_analyst + total_credits_agents
    total_all_cost = total_cost_code + total_cost_aifn + (total_credits_search + total_credits_analyst + total_credits_agents) * price_per_credit
    st.caption(f"**Total across all AI products:** {total_all_credits:,.1f} credits — **${total_all_cost:,.2f}** estimated cost")

    st.divider()

    combined_parts = []
    if not daily_code.empty:
        code_agg = daily_code.groupby("usage_date")["credits"].sum().reset_index()
        code_agg["product"] = "Cortex Code"
        combined_parts.append(code_agg)
    if not aifn_filtered.empty:
        aifn_agg = aifn_filtered.groupby("usage_date")["credits"].sum().reset_index()
        aifn_agg["usage_date"] = pd.to_datetime(aifn_agg["usage_date"])
        aifn_agg["product"] = "AI Functions"
        combined_parts.append(aifn_agg)
    if not search_filtered.empty:
        search_agg = search_filtered.groupby("usage_date")["credits"].sum().reset_index()
        search_agg["usage_date"] = pd.to_datetime(search_agg["usage_date"])
        search_agg["product"] = "Cortex Search"
        combined_parts.append(search_agg)
    if not analyst_filtered.empty:
        analyst_agg = analyst_filtered.groupby("usage_date")["credits"].sum().reset_index()
        analyst_agg["usage_date"] = pd.to_datetime(analyst_agg["usage_date"])
        analyst_agg["product"] = "Cortex Analyst"
        combined_parts.append(analyst_agg)
    if not agents_filtered.empty:
        agents_agg = agents_filtered.groupby("usage_date")["credits"].sum().reset_index()
        agents_agg["usage_date"] = pd.to_datetime(agents_agg["usage_date"])
        agents_agg["product"] = "Agents & SI"
        combined_parts.append(agents_agg)

    if combined_parts:
        combined_daily = pd.concat(combined_parts, ignore_index=True)
        ov_chart = (
            alt.Chart(combined_daily)
            .mark_bar()
            .encode(
                x=alt.X("usage_date:T", title=None),
                y=alt.Y("credits:Q", title="Credits"),
                color=alt.Color("product:N", legend=alt.Legend(orient="bottom", title="Product")),
                tooltip=[
                    alt.Tooltip("usage_date:T", title="Date", format="%Y-%m-%d"),
                    alt.Tooltip("product:N", title="Product"),
                    alt.Tooltip("credits:Q", title="Credits", format=",.1f"),
                ],
            )
            .properties(height=CHART_HEIGHT)
        )
        st.altair_chart(ov_chart, use_container_width=True)

    st.divider()
    st.subheader("Chargeback — credits & cost by user")
    st.caption("Covers products with user-level attribution: Cortex Code, AI Functions, Cortex Analyst, Agents & SI. Cortex Search is billed per-service and shown separately below.")

    cb = {}
    if not filtered.empty:
        for user, grp in filtered.groupby("user_name"):
            if pd.notna(user):
                cb.setdefault(str(user), {})["code_credits"] = float(grp["token_credits"].sum())
                cb.setdefault(str(user), {})["code_tokens"] = float(grp["tokens"].sum())
    if not aifn_filtered.empty:
        for user, grp in aifn_filtered.groupby("user_name"):
            if pd.notna(user):
                cb.setdefault(str(user), {})["aifn_credits"] = float(grp["credits"].sum())
                cb.setdefault(str(user), {})["aifn_calls"] = float(grp["calls"].sum())
    if not analyst_filtered.empty:
        for user, grp in analyst_filtered.groupby("user_name"):
            if pd.notna(user):
                cb.setdefault(str(user), {})["analyst_credits"] = float(grp["credits"].sum())
                cb.setdefault(str(user), {})["analyst_requests"] = float(grp["requests"].sum())
    if not agents_filtered.empty:
        for user, grp in agents_filtered.groupby("user_name"):
            if pd.notna(user):
                cb.setdefault(str(user), {})["agents_credits"] = float(grp["credits"].sum())
                cb.setdefault(str(user), {})["agents_tokens"] = float(grp["tokens"].sum())

    if cb:
        cb_rows = []
        credit_cols = ["code_credits", "aifn_credits", "analyst_credits", "agents_credits"]
        for user, vals in cb.items():
            total_cr = sum(vals.get(c, 0.0) for c in credit_cols)
            cb_rows.append({
                "User": user,
                "Cortex Code Credits": vals.get("code_credits", 0.0),
                "Cortex Code Tokens": vals.get("code_tokens", 0.0),
                "AI Functions Credits": vals.get("aifn_credits", 0.0),
                "AI Functions Calls": vals.get("aifn_calls", 0.0),
                "Analyst Credits": vals.get("analyst_credits", 0.0),
                "Analyst Requests": vals.get("analyst_requests", 0.0),
                "Agents & SI Credits": vals.get("agents_credits", 0.0),
                "Agents & SI Tokens": vals.get("agents_tokens", 0.0),
                "Total Credits": total_cr,
                "Est. Total Cost ($)": (
                    vals.get("aifn_credits", 0.0) * customer_credit_price
                    + sum(vals.get(c, 0.0) for c in ["code_credits", "analyst_credits", "agents_credits"]) * price_per_credit
                ),
            })
        cb_df = pd.DataFrame(cb_rows).fillna(0).sort_values("Total Credits", ascending=False)

        st.dataframe(
            cb_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Cortex Code Credits": st.column_config.NumberColumn(format="%.1f"),
                "Cortex Code Tokens": st.column_config.NumberColumn(format="%,.0f"),
                "AI Functions Credits": st.column_config.NumberColumn(format="%.1f"),
                "AI Functions Calls": st.column_config.NumberColumn(format="%,.0f"),
                "Analyst Credits": st.column_config.NumberColumn(format="%.1f"),
                "Analyst Requests": st.column_config.NumberColumn(format="%,.0f"),
                "Agents & SI Credits": st.column_config.NumberColumn(format="%.1f"),
                "Agents & SI Tokens": st.column_config.NumberColumn(format="%,.0f"),
                "Total Credits": st.column_config.NumberColumn(format="%.1f"),
                "Est. Total Cost ($)": st.column_config.NumberColumn(format="$%.2f"),
            },
        )
        csv_bytes = cb_df.to_csv(index=False).encode()
        st.download_button(
            ":material/download: Download chargeback CSV",
            data=csv_bytes,
            file_name="ai_spend_chargeback.csv",
            mime="text/csv",
        )
    else:
        st.info("No usage data available for chargeback.")

    if not search_filtered.empty:
        st.divider()
        st.subheader("Cortex Search — spend by service")
        search_svc = (
            search_filtered.groupby(["database_name", "schema_name", "service_name"])
            .agg(credits=("credits", "sum"))
            .reset_index()
        )
        search_svc["Est. Cost ($)"] = search_svc["credits"] * price_per_credit
        st.dataframe(
            search_svc.rename(columns={
                "database_name": "Database", "schema_name": "Schema",
                "service_name": "Service", "credits": "Credits",
            }).sort_values("Credits", ascending=False),
            use_container_width=True, hide_index=True,
            column_config={
                "Credits": st.column_config.NumberColumn(format="%.1f"),
                "Est. Cost ($)": st.column_config.NumberColumn(format="$%.2f"),
            },
        )

with tab_overview:
    render_overview()


@st.fragment
def render_cortex_code():
    (sub_credits, sub_cost, sub_users, sub_models, sub_detail) = st.tabs([
        ":material/toll: Credits",
        ":material/payments: Cost",
        ":material/group: Users",
        ":material/model_training: Models",
        ":material/table: Detail",
    ])

    with sub_credits:
        if daily_code.empty:
            st.info("No Cortex Code data for the selected filters.")
        else:
            chart = (
                alt.Chart(daily_code)
                .mark_bar()
                .encode(
                    x=alt.X("usage_date:T", title=None),
                    y=alt.Y("credits:Q", title="Credits"),
                    color=alt.Color("source:N", legend=alt.Legend(orient="bottom")),
                    tooltip=[
                        alt.Tooltip("usage_date:T", title="Date", format="%Y-%m-%d"),
                        alt.Tooltip("source:N", title="Source"),
                        alt.Tooltip("credits:Q", title="Credits", format=",.1f"),
                    ],
                )
                .properties(height=CHART_HEIGHT)
            )
            st.altair_chart(chart, use_container_width=True)

    with sub_cost:
        if daily_code.empty:
            st.info("No Cortex Code data for the selected filters.")
        else:
            cost_chart = (
                alt.Chart(daily_code)
                .mark_bar()
                .encode(
                    x=alt.X("usage_date:T", title=None),
                    y=alt.Y("cost:Q", title="Estimated cost ($)"),
                    color=alt.Color("source:N", legend=alt.Legend(orient="bottom")),
                    tooltip=[
                        alt.Tooltip("usage_date:T", title="Date", format="%Y-%m-%d"),
                        alt.Tooltip("source:N", title="Source"),
                        alt.Tooltip("cost:Q", title="Cost", format="$,.2f"),
                    ],
                )
                .properties(height=CHART_HEIGHT)
            )
            st.altair_chart(cost_chart, use_container_width=True)

    with sub_users:
        if filtered.empty:
            st.info("No Cortex Code data for the selected filters.")
        else:
            user_summary = (
                filtered.groupby("user_name")
                .agg(credits=("token_credits", "sum"), tokens=("tokens", "sum"), requests=("request_id", "nunique"))
                .reset_index()
                .sort_values("credits", ascending=False)
            )
            user_summary["cost"] = user_summary["credits"] * price_per_credit
            user_summary["tokens_per_credit"] = (user_summary["tokens"] / user_summary["credits"]).where(user_summary["credits"] > 0, 0)
            user_summary = user_summary.rename(columns={
                "user_name": "User", "credits": "Credits", "cost": "Est. cost ($)",
                "tokens": "Tokens", "requests": "Requests", "tokens_per_credit": "Tokens/Credit",
            })
            user_bar = (
                alt.Chart(user_summary)
                .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                .encode(
                    x=alt.X("User:N", sort="-y", title=None),
                    y=alt.Y("Est. cost ($):Q", title="Estimated cost ($)"),
                    color=alt.Color("User:N", legend=None),
                    tooltip=[
                        alt.Tooltip("User:N"),
                        alt.Tooltip("Credits:Q", format=",.1f"),
                        alt.Tooltip("Est. cost ($):Q", format="$,.2f"),
                        alt.Tooltip("Requests:Q", format=","),
                        alt.Tooltip("Tokens/Credit:Q", format=",.0f"),
                    ],
                )
                .properties(height=CHART_HEIGHT)
            )
            st.altair_chart(user_bar, use_container_width=True)
            st.dataframe(
                user_summary,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Credits": st.column_config.NumberColumn(format="%.1f"),
                    "Est. cost ($)": st.column_config.NumberColumn(format="$%.2f"),
                    "Tokens": st.column_config.NumberColumn(format="%d"),
                    "Requests": st.column_config.NumberColumn(format="%d"),
                    "Tokens/Credit": st.column_config.NumberColumn(format="%,.0f"),
                },
            )

    with sub_models:
        if model_summary.empty:
            st.info("No model-level granular data available.")
        else:
            st.subheader("Credits by model")
            token_type_data = []
            for _, row in model_summary.iterrows():
                for tt, label in TOKEN_TYPE_LABELS.items():
                    token_type_data.append({
                        "Model": row["model_name"],
                        "Token type": label,
                        "Credits": row.get(f"{tt}_credits", 0) or 0,
                        "Tokens": row.get(f"{tt}_tokens", 0) or 0,
                    })
            tt_df = pd.DataFrame(token_type_data)
            model_bar = (
                alt.Chart(tt_df)
                .mark_bar()
                .encode(
                    x=alt.X("Model:N", sort="-y", title=None),
                    y=alt.Y("Credits:Q", title="Credits"),
                    color=alt.Color("Token type:N", legend=alt.Legend(orient="bottom"),
                                    scale=alt.Scale(domain=list(TOKEN_TYPE_LABELS.values()))),
                    tooltip=[
                        alt.Tooltip("Model:N"),
                        alt.Tooltip("Token type:N"),
                        alt.Tooltip("Credits:Q", format=",.1f"),
                        alt.Tooltip("Tokens:Q", format=",.0f"),
                    ],
                )
                .properties(height=CHART_HEIGHT)
            )
            st.altair_chart(model_bar, use_container_width=True)
            st.subheader("Model summary")
            display_models = model_summary[[
                "model_name", "requests", "total_tokens", "total_credits", "total_cost",
                "input_tokens", "cache_read_tokens", "cache_write_tokens", "output_tokens",
            ]].rename(columns={
                "model_name": "Model", "requests": "Requests",
                "total_tokens": "Total tokens", "total_credits": "Total credits",
                "total_cost": "Est. cost ($)",
                "input_tokens": "Input tokens", "cache_read_tokens": "Cache read tokens",
                "cache_write_tokens": "Cache write tokens", "output_tokens": "Output tokens",
            }).sort_values("Total credits", ascending=False)
            st.dataframe(
                display_models,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Total tokens": st.column_config.NumberColumn(format="%,.0f"),
                    "Total credits": st.column_config.NumberColumn(format="%.1f"),
                    "Est. cost ($)": st.column_config.NumberColumn(format="$%.2f"),
                    "Input tokens": st.column_config.NumberColumn(format="%,.0f"),
                    "Cache read tokens": st.column_config.NumberColumn(format="%,.0f"),
                    "Cache write tokens": st.column_config.NumberColumn(format="%,.0f"),
                    "Output tokens": st.column_config.NumberColumn(format="%,.0f"),
                    "Requests": st.column_config.NumberColumn(format="%d"),
                },
            )

    with sub_detail:
        if filtered.empty:
            st.info("No Cortex Code data for the selected filters.")
        else:
            detail = (
                filtered[["usage_date", "source", "user_name", "token_credits", "tokens", "request_id"]]
                .copy()
                .rename(columns={
                    "usage_date": "Date", "source": "Source", "user_name": "User",
                    "token_credits": "Credits", "tokens": "Tokens", "request_id": "Request ID",
                })
                .sort_values("Date", ascending=False)
            )
            detail["Cost"] = detail["Credits"] * price_per_credit
            detail["Tokens/Credit"] = (detail["Tokens"] / detail["Credits"]).where(detail["Credits"] > 0, 0)
            st.dataframe(
                detail,
                use_container_width=True,
                hide_index=True,
                height=500,
                column_config={
                    "Credits": st.column_config.NumberColumn(format="%.1f"),
                    "Cost": st.column_config.NumberColumn(format="$%.2f"),
                    "Tokens": st.column_config.NumberColumn(format="%d"),
                    "Tokens/Credit": st.column_config.NumberColumn(format="%,.0f"),
                },
            )

with tab_code:
    render_cortex_code()


@st.fragment
def render_ai_functions():
    if aifn_filtered.empty:
        st.info("No AI Functions usage data found in `SNOWFLAKE.ACCOUNT_USAGE.CORTEX_AI_FUNCTIONS_USAGE_HISTORY`.")
    else:
        aifn_daily = (
            aifn_filtered.groupby(["usage_date", "function_name"])
            .agg(credits=("credits", "sum"), calls=("calls", "sum"))
            .reset_index()
        )
        aifn_daily["usage_date"] = pd.to_datetime(aifn_daily["usage_date"])
        aifn_daily["cost"] = aifn_daily["credits"] * customer_credit_price

        m1, m2, m3 = st.columns(3)
        m1.metric("Total credits", f"{total_credits_aifn:,.1f}")
        m2.metric("Estimated cost", f"${total_cost_aifn:,.2f}")
        m3.metric("Total calls", f"{aifn_filtered['calls'].sum():,.0f}")

        st.subheader("Daily credits by function")
        fn_chart = (
            alt.Chart(aifn_daily)
            .mark_bar()
            .encode(
                x=alt.X("usage_date:T", title=None),
                y=alt.Y("credits:Q", title="Credits"),
                color=alt.Color("function_name:N", legend=alt.Legend(orient="bottom", title="Function")),
                tooltip=[
                    alt.Tooltip("usage_date:T", title="Date", format="%Y-%m-%d"),
                    alt.Tooltip("function_name:N", title="Function"),
                    alt.Tooltip("credits:Q", title="Credits", format=",.1f"),
                    alt.Tooltip("calls:Q", title="Calls", format=","),
                ],
            )
            .properties(height=CHART_HEIGHT)
        )
        st.altair_chart(fn_chart, use_container_width=True)

        fn_col, model_col = st.columns(2)

        with fn_col:
            st.subheader("By function")
            fn_summary = (
                aifn_filtered.groupby("function_name")
                .agg(credits=("credits", "sum"), calls=("calls", "sum"))
                .reset_index()
                .sort_values("credits", ascending=False)
            )
            fn_summary["Est. cost ($)"] = fn_summary["credits"] * customer_credit_price
            fn_summary = fn_summary.rename(columns={
                "function_name": "Function", "credits": "Credits", "calls": "Calls",
            })
            st.dataframe(fn_summary, use_container_width=True, hide_index=True,
                column_config={
                    "Credits": st.column_config.NumberColumn(format="%.1f"),
                    "Est. cost ($)": st.column_config.NumberColumn(format="$%.2f"),
                    "Calls": st.column_config.NumberColumn(format="%,.0f"),
                })

        with model_col:
            st.subheader("By model")
            model_fn_summary = (
                aifn_filtered[aifn_filtered["model_name"] != "N/A"]
                .groupby("model_name")
                .agg(credits=("credits", "sum"), calls=("calls", "sum"))
                .reset_index()
                .sort_values("credits", ascending=False)
            )
            if not model_fn_summary.empty:
                model_fn_summary["Est. cost ($)"] = model_fn_summary["credits"] * customer_credit_price
                model_fn_summary = model_fn_summary.rename(columns={
                    "model_name": "Model", "credits": "Credits", "calls": "Calls",
                })
                st.dataframe(model_fn_summary, use_container_width=True, hide_index=True,
                    column_config={
                        "Credits": st.column_config.NumberColumn(format="%.1f"),
                        "Est. cost ($)": st.column_config.NumberColumn(format="$%.2f"),
                        "Calls": st.column_config.NumberColumn(format="%,.0f"),
                    })
            else:
                st.info("No model-level data available.")

        st.subheader("By user")
        aifn_user = (
            aifn_filtered.groupby("user_name")
            .agg(credits=("credits", "sum"), calls=("calls", "sum"))
            .reset_index()
            .sort_values("credits", ascending=False)
        )
        aifn_user["Est. cost ($)"] = aifn_user["credits"] * customer_credit_price
        aifn_user = aifn_user.rename(columns={
            "user_name": "User", "credits": "Credits", "calls": "Calls",
        })
        st.dataframe(aifn_user, use_container_width=True, hide_index=True,
            column_config={
                "Credits": st.column_config.NumberColumn(format="%.1f"),
                "Est. cost ($)": st.column_config.NumberColumn(format="$%.2f"),
                "Calls": st.column_config.NumberColumn(format="%,.0f"),
            })
        st.caption("AI Functions are billed on traditional credits at your customer credit price.")

with tab_aifn:
    render_ai_functions()


@st.fragment
def render_cortex_search():
    if search_filtered.empty:
        st.info("No Cortex Search usage data found in `SNOWFLAKE.ACCOUNT_USAGE.CORTEX_SEARCH_SERVING_USAGE_HISTORY`.")
    else:
        m1, m2 = st.columns(2)
        m1.metric("Total credits", f"{total_credits_search:,.1f}")
        m2.metric("Estimated cost", f"${total_credits_search * price_per_credit:,.2f}")
        st.info("Cortex Search is billed per-service. User-level attribution is not available from `ACCOUNT_USAGE`.")

        st.subheader("Daily credits by service")
        search_daily = (
            search_filtered.groupby(["usage_date", "service_name"])
            .agg(credits=("credits", "sum"))
            .reset_index()
        )
        search_daily["usage_date"] = pd.to_datetime(search_daily["usage_date"])
        search_chart = (
            alt.Chart(search_daily)
            .mark_bar()
            .encode(
                x=alt.X("usage_date:T", title=None),
                y=alt.Y("credits:Q", title="Credits"),
                color=alt.Color("service_name:N", legend=alt.Legend(orient="bottom", title="Service")),
                tooltip=[
                    alt.Tooltip("usage_date:T", title="Date", format="%Y-%m-%d"),
                    alt.Tooltip("service_name:N", title="Service"),
                    alt.Tooltip("credits:Q", title="Credits", format=",.1f"),
                ],
            )
            .properties(height=CHART_HEIGHT)
        )
        st.altair_chart(search_chart, use_container_width=True)

        st.subheader("By service")
        search_svc = (
            search_filtered.groupby(["database_name", "schema_name", "service_name"])
            .agg(credits=("credits", "sum"))
            .reset_index()
            .sort_values("credits", ascending=False)
        )
        search_svc["Est. cost ($)"] = search_svc["credits"] * price_per_credit
        search_svc = search_svc.rename(columns={
            "database_name": "Database", "schema_name": "Schema",
            "service_name": "Service", "credits": "Credits",
        })
        st.dataframe(search_svc, use_container_width=True, hide_index=True,
            column_config={
                "Credits": st.column_config.NumberColumn(format="%.1f"),
                "Est. cost ($)": st.column_config.NumberColumn(format="$%.2f"),
            })

with tab_search:
    render_cortex_search()


@st.fragment
def render_cortex_analyst():
    if analyst_filtered.empty:
        st.info("No Cortex Analyst usage data found in `SNOWFLAKE.ACCOUNT_USAGE.CORTEX_ANALYST_USAGE_HISTORY`.")
    else:
        total_analyst_requests = analyst_filtered["requests"].sum()
        avg_cost_per_req = (total_credits_analyst * price_per_credit / total_analyst_requests) if total_analyst_requests > 0 else 0

        m1, m2, m3 = st.columns(3)
        m1.metric("Total credits", f"{total_credits_analyst:,.1f}")
        m2.metric("Estimated cost", f"${total_credits_analyst * price_per_credit:,.2f}")
        m3.metric("Total requests", f"{total_analyst_requests:,.0f}")

        st.subheader("Daily credits by user")
        analyst_daily = (
            analyst_filtered.groupby(["usage_date", "user_name"])
            .agg(credits=("credits", "sum"), requests=("requests", "sum"))
            .reset_index()
        )
        analyst_daily["usage_date"] = pd.to_datetime(analyst_daily["usage_date"])
        analyst_chart = (
            alt.Chart(analyst_daily)
            .mark_bar()
            .encode(
                x=alt.X("usage_date:T", title=None),
                y=alt.Y("credits:Q", title="Credits"),
                color=alt.Color("user_name:N", legend=alt.Legend(orient="bottom", title="User")),
                tooltip=[
                    alt.Tooltip("usage_date:T", title="Date", format="%Y-%m-%d"),
                    alt.Tooltip("user_name:N", title="User"),
                    alt.Tooltip("credits:Q", title="Credits", format=",.1f"),
                    alt.Tooltip("requests:Q", title="Requests", format=","),
                ],
            )
            .properties(height=CHART_HEIGHT)
        )
        st.altair_chart(analyst_chart, use_container_width=True)

        st.subheader("By user")
        analyst_user = (
            analyst_filtered.groupby("user_name")
            .agg(credits=("credits", "sum"), requests=("requests", "sum"))
            .reset_index()
            .sort_values("credits", ascending=False)
        )
        analyst_user["Est. cost ($)"] = analyst_user["credits"] * price_per_credit
        analyst_user["Avg cost/request ($)"] = (analyst_user["Est. cost ($)"] / analyst_user["requests"]).where(analyst_user["requests"] > 0, 0)
        analyst_user = analyst_user.rename(columns={
            "user_name": "User", "credits": "Credits", "requests": "Requests",
        })
        st.dataframe(analyst_user, use_container_width=True, hide_index=True,
            column_config={
                "Credits": st.column_config.NumberColumn(format="%.1f"),
                "Est. cost ($)": st.column_config.NumberColumn(format="$%.2f"),
                "Requests": st.column_config.NumberColumn(format="%,.0f"),
                "Avg cost/request ($)": st.column_config.NumberColumn(format="$%.4f"),
            })

with tab_analyst:
    render_cortex_analyst()


@st.fragment
def render_agents():
    if agents_filtered.empty:
        st.info("No Agents or Snowflake Intelligence usage data found.")
    else:
        total_agents_tokens = agents_filtered["tokens"].sum()

        m1, m2, m3 = st.columns(3)
        m1.metric("Total credits", f"{total_credits_agents:,.1f}")
        m2.metric("Estimated cost", f"${total_credits_agents * price_per_credit:,.2f}")
        m3.metric("Total tokens", f"{total_agents_tokens:,.0f}")

        st.subheader("Daily credits by product")
        agents_daily = (
            agents_filtered.groupby(["usage_date", "product"])
            .agg(credits=("credits", "sum"), tokens=("tokens", "sum"))
            .reset_index()
        )
        agents_daily["usage_date"] = pd.to_datetime(agents_daily["usage_date"])
        agents_chart = (
            alt.Chart(agents_daily)
            .mark_bar()
            .encode(
                x=alt.X("usage_date:T", title=None),
                y=alt.Y("credits:Q", title="Credits"),
                color=alt.Color("product:N", legend=alt.Legend(orient="bottom", title="Product")),
                tooltip=[
                    alt.Tooltip("usage_date:T", title="Date", format="%Y-%m-%d"),
                    alt.Tooltip("product:N", title="Product"),
                    alt.Tooltip("credits:Q", title="Credits", format=",.1f"),
                    alt.Tooltip("tokens:Q", title="Tokens", format=","),
                ],
            )
            .properties(height=CHART_HEIGHT)
        )
        st.altair_chart(agents_chart, use_container_width=True)

        st.subheader("By user")
        agents_user = (
            agents_filtered.groupby(["user_name", "product"])
            .agg(credits=("credits", "sum"), tokens=("tokens", "sum"))
            .reset_index()
            .sort_values("credits", ascending=False)
        )
        agents_user["Est. cost ($)"] = agents_user["credits"] * price_per_credit
        agents_user["tokens_per_credit"] = (agents_user["tokens"] / agents_user["credits"]).where(agents_user["credits"] > 0, 0)
        agents_user = agents_user.rename(columns={
            "user_name": "User", "product": "Product",
            "credits": "Credits", "tokens": "Tokens",
            "tokens_per_credit": "Tokens/Credit",
        })
        st.dataframe(agents_user, use_container_width=True, hide_index=True,
            column_config={
                "Credits": st.column_config.NumberColumn(format="%.1f"),
                "Est. cost ($)": st.column_config.NumberColumn(format="$%.2f"),
                "Tokens": st.column_config.NumberColumn(format="%,.0f"),
                "Tokens/Credit": st.column_config.NumberColumn(format="%,.0f"),
            })

        st.subheader("By resource (agent / SI)")
        agents_res = (
            agents_filtered.groupby(["resource_name", "product"])
            .agg(credits=("credits", "sum"), tokens=("tokens", "sum"))
            .reset_index()
            .sort_values("credits", ascending=False)
        )
        agents_res["Est. cost ($)"] = agents_res["credits"] * price_per_credit
        agents_res = agents_res.rename(columns={
            "resource_name": "Resource", "product": "Product",
            "credits": "Credits", "tokens": "Tokens",
        })
        st.dataframe(agents_res, use_container_width=True, hide_index=True,
            column_config={
                "Credits": st.column_config.NumberColumn(format="%.1f"),
                "Est. cost ($)": st.column_config.NumberColumn(format="$%.2f"),
                "Tokens": st.column_config.NumberColumn(format="%,.0f"),
            })

with tab_agents:
    render_agents()


@st.fragment
def render_estimator():
    st.subheader("Workload Cost Estimator")
    st.caption("Configure expected usage for each AI product to forecast monthly credits and cost. AI Functions use the customer credit price; other products use the AI credit tier price.")

    est_days = st.number_input("Projection period (days)", min_value=1, max_value=365, value=30, step=1, key="est_days")

    st.divider()
    col_code_e, col_aifn_e = st.columns(2)

    with col_code_e:
        st.write("##### Cortex Code")
        e_developers = st.number_input("Developers", 1, 10000, 10, key="e_devs")
        e_req_per_dev = st.number_input("Requests per developer per day", 1, 1000, 50, key="e_req")
        e_code_model = st.selectbox("Model", list(CORTEX_CODE_PRICING.keys()), key="e_code_model")
        e_input_tok = st.number_input("Avg input tokens per request", 100, 1_000_000, 2000, step=100, key="e_inp")
        e_output_tok = st.number_input("Avg output tokens per request", 10, 100_000, 500, step=50, key="e_out")
        code_rates = CORTEX_CODE_PRICING[e_code_model]
        e_code_total_req = e_developers * e_req_per_dev * est_days
        e_code_inp_tok = e_code_total_req * e_input_tok
        e_code_out_tok = e_code_total_req * e_output_tok
        e_code_credits = (e_code_inp_tok / 1_000_000 * code_rates["input"]) + (e_code_out_tok / 1_000_000 * code_rates["output"])
        e_code_cost = e_code_credits * price_per_credit
        st.metric("Estimated credits", f"{e_code_credits:,.2f}", f"${e_code_cost:,.2f}")

    with col_aifn_e:
        st.write("##### AI Functions")
        e_fn_type = st.selectbox("Function type", AI_FUNCTION_TYPES, key="e_fn_type")
        e_fn_calls = st.number_input("Calls per day", 1, 1_000_000, 1000, step=100, key="e_fn_calls")
        if e_fn_type == "AI_COMPLETE":
            e_fn_model = st.selectbox("Model", list(AI_COMPLETE_PRICING.keys()), key="e_fn_model")
            e_fn_tokens = st.number_input("Avg tokens per call", 10, 100_000, 500, step=50, key="e_fn_tokens")
            fn_rates = AI_COMPLETE_PRICING[e_fn_model]
            e_fn_credits = (e_fn_calls * est_days * e_fn_tokens / 1_000_000
                            * (fn_rates["input"] * 0.7 + fn_rates["output"] * 0.3))
        else:
            e_fn_cpc = st.number_input("Credits per call (estimate)", 0.0001, 1.0, 0.005, format="%.4f", key="e_fn_cpc")
            e_fn_credits = e_fn_calls * est_days * e_fn_cpc
        e_fn_cost = e_fn_credits * customer_credit_price
        st.metric("Estimated credits", f"{e_fn_credits:,.2f}", f"${e_fn_cost:,.2f}")

    col_search_e, col_analyst_e = st.columns(2)

    with col_search_e:
        st.write("##### Cortex Search")
        e_queries = st.number_input("Queries per day", 1, 10_000_000, 10000, step=1000, key="e_queries")
        e_search_rate = st.number_input("Credits per 1K queries", 0.01, 10.0, CORTEX_SEARCH_RATE_PER_1K, format="%.3f", key="e_search_rate",
                                        help="Default: 0.25 credits per 1K queries (published rate)")
        e_search_credits = e_queries * est_days / 1000 * e_search_rate
        e_search_cost = e_search_credits * price_per_credit
        st.metric("Estimated credits", f"{e_search_credits:,.2f}", f"${e_search_cost:,.2f}")

    with col_analyst_e:
        st.write("##### Cortex Analyst")
        e_analyst_req = st.number_input("Requests per day", 1, 100_000, 100, step=10, key="e_analyst_req")
        e_analyst_rate = st.number_input("Credits per request", 0.001, 10.0, CORTEX_ANALYST_RATE_PER_REQUEST, format="%.4f", key="e_analyst_rate",
                                         help="Default: 0.06 credits per request (published rate)")
        e_analyst_credits = e_analyst_req * est_days * e_analyst_rate
        e_analyst_cost = e_analyst_credits * price_per_credit
        st.metric("Estimated credits", f"{e_analyst_credits:,.2f}", f"${e_analyst_cost:,.2f}")

    col_agents_e, _ = st.columns(2)
    with col_agents_e:
        st.write("##### Agents & Snowflake Intelligence")
        e_agent_req = st.number_input("Requests per day", 1, 100_000, 50, step=10, key="e_agent_req")
        e_agent_model = st.selectbox("Model", list(CORTEX_CODE_PRICING.keys()), key="e_agent_model")
        e_agent_tokens = st.number_input("Avg tokens per request", 100, 1_000_000, 5000, step=100, key="e_agent_tokens")
        agent_rates = CORTEX_CODE_PRICING[e_agent_model]
        e_agent_total_req = e_agent_req * est_days
        e_agent_inp = e_agent_total_req * e_agent_tokens * 0.7
        e_agent_out = e_agent_total_req * e_agent_tokens * 0.3
        e_agent_credits = (e_agent_inp / 1_000_000 * agent_rates["input"]) + (e_agent_out / 1_000_000 * agent_rates["output"])
        e_agent_cost = e_agent_credits * price_per_credit
        st.metric("Estimated credits", f"{e_agent_credits:,.2f}", f"${e_agent_cost:,.2f}")

    st.divider()
    st.subheader("Forecasted cost breakdown")

    est_total_credits = e_code_credits + e_fn_credits + e_search_credits + e_analyst_credits + e_agent_credits
    est_total_cost = e_code_cost + e_fn_cost + e_search_cost + e_analyst_cost + e_agent_cost

    summary_rows = [
        {"Product": "Cortex Code", "Credits": e_code_credits, "Est. Cost ($)": e_code_cost},
        {"Product": "AI Functions", "Credits": e_fn_credits, "Est. Cost ($)": e_fn_cost},
        {"Product": "Cortex Search", "Credits": e_search_credits, "Est. Cost ($)": e_search_cost},
        {"Product": "Cortex Analyst", "Credits": e_analyst_credits, "Est. Cost ($)": e_analyst_cost},
        {"Product": "Agents & SI", "Credits": e_agent_credits, "Est. Cost ($)": e_agent_cost},
        {"Product": "TOTAL", "Credits": est_total_credits, "Est. Cost ($)": est_total_cost},
    ]
    est_df = pd.DataFrame(summary_rows)

    e1, e2, e3 = st.columns(3)
    e1.metric(f"Total credits ({est_days}d)", f"{est_total_credits:,.2f}")
    e2.metric(f"Total estimated cost ({est_days}d)", f"${est_total_cost:,.2f}")
    e3.metric("Per-month run rate", f"${est_total_cost / est_days * 30:,.2f}")

    st.dataframe(
        est_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Credits": st.column_config.NumberColumn(format="%.1f"),
            "Est. Cost ($)": st.column_config.NumberColumn(format="$%.2f"),
        },
    )

    if est_total_credits > 0:
        pie_data = est_df[est_df["Product"] != "TOTAL"][["Product", "Est. Cost ($)"]].copy()
        pie_chart = (
            alt.Chart(pie_data)
            .mark_arc(innerRadius=60)
            .encode(
                theta=alt.Theta("Est. Cost ($):Q"),
                color=alt.Color("Product:N", legend=alt.Legend(orient="right")),
                tooltip=[
                    alt.Tooltip("Product:N"),
                    alt.Tooltip("Est. Cost ($):Q", format="$,.2f"),
                ],
            )
            .properties(height=280, title=f"Cost distribution ({est_days}d)")
        )
        st.altair_chart(pie_chart, use_container_width=True)

    st.download_button(
        ":material/download: Download estimate as CSV",
        data=est_df.to_csv(index=False).encode(),
        file_name="ai_spend_estimate.csv",
        mime="text/csv",
    )

with tab_estimator:
    render_estimator()


@st.fragment
def render_pricing():
    st.subheader("Snowflake AI Features Credit Table")
    st.caption("Table 6(g): Cortex Code — Credits per 1M tokens by model and token type")

    active_pricing = st.session_state.get("refreshed_pricing", CORTEX_CODE_PRICING)
    pricing_rows = []
    for model, rates in active_pricing.items():
        pricing_rows.append({
            "Model": model,
            "Input": rates["input"],
            "Cache Read": rates["cache_read_input"],
            "Cache Write": rates["cache_write_input"],
            "Output": rates["output"],
        })
    pricing_df = pd.DataFrame(pricing_rows)
    st.dataframe(
        pricing_df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Input": st.column_config.NumberColumn(format="%.2f", help="Credits per 1M input tokens"),
            "Cache Read": st.column_config.NumberColumn(format="%.2f", help="Credits per 1M cache read tokens"),
            "Cache Write": st.column_config.NumberColumn(format="%.2f", help="Credits per 1M cache write tokens"),
            "Output": st.column_config.NumberColumn(format="%.2f", help="Credits per 1M output tokens"),
        },
    )

    st.divider()
    st.warning(
        "**Pricing is subject to change.** The rates shown above are extracted from the "
        "[Snowflake Service Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf) "
        "and may not reflect the latest published values. Always verify against your Snowflake contract or the official documentation.",
        icon=":material/warning:",
    )

    st.subheader("Keeping prices up to date")
    st.markdown(
        "The **Refresh from PDF** button below attempts to download the Consumption Table PDF directly "
        "from Snowflake's website and extract Table 6(g) using `CORTEX AI_PARSE_DOCUMENT`. "
        "This may fail if the PDF URL changes or if outbound network access is restricted.\n\n"
        "**Alternative update methods:**\n"
        "1. **Manual download & stage upload** — Download "
        "[CreditConsumptionTable.pdf](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf) "
        "yourself, then upload it to the Snowflake stage:\n"
        "   ```sql\n"
        f"   PUT file:///path/to/CreditConsumptionTable.pdf @{PRICING_STAGE} AUTO_COMPRESS=FALSE OVERWRITE=TRUE;\n"
        "   ```\n"
        "   You can also upload the file through the **Snowsight UI**: navigate to "
        f"**Data → Databases → `{PRICING_STAGE.split('.')[0]}` → `{PRICING_STAGE.split('.')[1]}` → Stages → "
        f"`{PRICING_STAGE.split('.')[2]}`** and use the **+ Files** button to upload the PDF.\n\n"
        f"   > **Stage location:** `@{PRICING_STAGE}` — to change this, update the `PRICING_STAGE` "
        "constant at the top of `streamlit_app.py`. The stage must use `ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')` "
        "for `AI_PARSE_DOCUMENT` compatibility.\n"
        "2. **Edit the built-in defaults** — Update the `CORTEX_CODE_PRICING` dictionary in "
        "`streamlit_app.py` directly if you prefer a static configuration."
    )

    refresh_col, stage_col, info_col = st.columns([1, 1, 2])
    with refresh_col:
        if not IS_SIS:
            if st.button(":material/refresh: Refresh from PDF", help="Download and extract Table 6(g) using Cortex AI_PARSE_DOCUMENT"):
                with st.spinner("Downloading PDF and extracting pricing..."):
                    try:
                        new_pricing = refresh_pricing_from_pdf()
                        if new_pricing:
                            st.session_state["refreshed_pricing"] = new_pricing
                            st.success(f"Refreshed pricing for {len(new_pricing)} models.")
                            st.rerun()
                        else:
                            st.error("Could not extract Table 6(g) from the PDF.")
                    except Exception as e:
                        st.error(f"Refresh failed: {e}")
    with stage_col:
        if st.button(":material/refresh: Refresh from Stage", help=f"Parse CreditConsumptionTable.pdf from @{PRICING_STAGE}"):
            with st.spinner("Extracting pricing from staged PDF..."):
                try:
                    new_pricing = refresh_pricing_from_stage()
                    if new_pricing:
                        st.session_state["refreshed_pricing"] = new_pricing
                        st.success(f"Refreshed pricing for {len(new_pricing)} models.")
                        st.rerun()
                    else:
                        st.error("Could not extract Table 6(g) from the staged PDF.")
                except Exception as e:
                    st.error(f"Stage refresh failed: {e}")
    with info_col:
        if "refreshed_pricing" in st.session_state:
            st.caption(":material/check_circle: Pricing refreshed from live PDF via Cortex AI_PARSE_DOCUMENT")
        else:
            st.caption(":material/info: Showing built-in pricing defaults.")

    st.divider()
    st.subheader("Your observed rates vs. published rates")
    if not model_summary.empty:
        observed_rows = []
        for _, row in model_summary.iterrows():
            m = row["model_name"]
            published = active_pricing.get(m, {})
            obs_input = (row["input_credits"] / row["input_tokens"] * 1_000_000) if row["input_tokens"] > 0 else None
            obs_output = (row["output_credits"] / row["output_tokens"] * 1_000_000) if row["output_tokens"] > 0 else None
            observed_rows.append({
                "Model": m,
                "Input (observed)": obs_input,
                "Input (published)": published.get("input"),
                "Output (observed)": obs_output,
                "Output (published)": published.get("output"),
            })
        obs_df = pd.DataFrame(observed_rows)
        st.dataframe(obs_df, use_container_width=True, hide_index=True,
            column_config={col: st.column_config.NumberColumn(format="%.2f") for col in obs_df.columns if col != "Model"})
    else:
        st.info("No granular usage data available to compare observed vs. published rates.")
    st.caption("Source: [Snowflake Service Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf) — Table 6(g): Cortex Code")

with tab_pricing:
    render_pricing()

st.divider()
st.caption(
    f":material/info: Data from `SNOWFLAKE.ACCOUNT_USAGE`. "
    f"Cortex Code sources: {', '.join(available_sources)}. "
    f"AI credit price: **${price_per_credit:.2f}** · Customer credit price: **${customer_credit_price:.2f}**. "
    "Most AI products use the flat AI credit price. AI Functions bills on traditional credits at the customer rate."
)
