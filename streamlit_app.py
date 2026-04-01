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
    page_title="Cortex Code consumption",
    page_icon=":material/code:",
    layout="wide",
)

TIME_RANGES = ["1W", "1M", "3M", "6M", "YTD", "All"]
CHART_HEIGHT = 350
SOURCES = {"CLI": "CORTEX_CODE_CLI_USAGE_HISTORY", "Snowsight": "CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY"}
PRICING_CHANGE_DATE = date(2026, 4, 1)
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

CONSUMPTION_TABLE_URL = "https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf"
PRICING_STAGE = "CORTEX_CODE_DASHBOARD.PUBLIC.PRICING_DOCS"

TOKEN_TYPES = ["input", "cache_read", "cache_write", "output"]
TOKEN_TYPE_LABELS = {
    "input": "Input",
    "cache_read": "Cache Read",
    "cache_write": "Cache Write",
    "output": "Output",
}


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
            cur.execute(f"CREATE DATABASE IF NOT EXISTS CORTEX_CODE_DASHBOARD")
            cur.execute(f"CREATE SCHEMA IF NOT EXISTS CORTEX_CODE_DASHBOARD.PUBLIC")
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


def refresh_pricing_from_stage() -> dict:
    raw_json = run_scalar(f"""
        SELECT AI_PARSE_DOCUMENT(
            TO_FILE('@{PRICING_STAGE}', 'CreditConsumptionTable.pdf'),
            {{'mode': 'LAYOUT', 'page_split': true}}
        )::VARCHAR
    """)
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


def filter_by_time_range(df: pd.DataFrame, x_col: str, time_range: str) -> pd.DataFrame:
    if time_range == "All" or df.empty:
        return df
    df = df.copy()
    df[x_col] = pd.to_datetime(df[x_col])
    max_date = df[x_col].max()
    if time_range == "1W":
        min_date = max_date - timedelta(days=7)
    elif time_range == "1M":
        min_date = max_date - timedelta(days=30)
    elif time_range == "3M":
        min_date = max_date - timedelta(days=90)
    elif time_range == "6M":
        min_date = max_date - timedelta(days=180)
    elif time_range == "YTD":
        min_date = pd.Timestamp(date(max_date.year, 1, 1))
    else:
        return df
    return df[df[x_col] >= min_date]


@st.cache_data(ttl=600, show_spinner="Loading usage data...")
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


@st.cache_data(ttl=600, show_spinner="Loading model-level data...")
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


if not IS_SIS:
    get_conn()

hdr_left, hdr_right = st.columns([8, 2])
with hdr_left:
    st.title("Cortex Code consumption")
with hdr_right:
    if st.button("Reset"):
        st.cache_data.clear()
        st.session_state.clear()
        st.rerun()

raw = load_usage_data()
granular = load_granular_data()
available_sources = sorted(raw["source"].unique())

with st.sidebar:
    st.header("Settings")
    tier_labels = list(CREDIT_PRICE_TIERS.keys())
    selected_tier = st.selectbox("Credit pricing tier", tier_labels, index=0, key="tier")
    price_per_credit = CREDIT_PRICE_TIERS[selected_tier]
    time_range = st.selectbox("Time range", TIME_RANGES, index=TIME_RANGES.index("All"), key="tr")
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
    st.warning("No data matches the current filters.")
    st.stop()

total_credits = filtered["token_credits"].sum()
total_cost = total_credits * price_per_credit
total_requests = filtered["request_id"].nunique()
total_tokens = filtered["tokens"].sum()
active_users = filtered["user_name"].nunique()
tokens_per_credit = total_tokens / total_credits if total_credits > 0 else 0

if time_range != "All" and not raw.empty:
    prev_filtered = raw[raw["source"].isin(source_filter) & raw["user_name"].isin(user_filter)]
    current_min = filtered["usage_date"].min()
    current_max = filtered["usage_date"].max()
    span = (pd.to_datetime(current_max) - pd.to_datetime(current_min)).days
    if span > 0:
        prev_end = pd.to_datetime(current_min) - timedelta(days=1)
        prev_start = prev_end - timedelta(days=span)
        prev_period = prev_filtered[
            (pd.to_datetime(prev_filtered["usage_date"]) >= prev_start)
            & (pd.to_datetime(prev_filtered["usage_date"]) <= prev_end)
        ]
        prev_credits = prev_period["token_credits"].sum()
        prev_cost = prev_credits * price_per_credit
        prev_requests = prev_period["request_id"].nunique()
        prev_tokens = prev_period["tokens"].sum()
        prev_tpc = prev_tokens / prev_credits if prev_credits > 0 else 0
        delta_credits = f"{total_credits - prev_credits:+.4f}"
        delta_cost = f"${total_cost - prev_cost:+,.2f}"
        delta_requests = f"{total_requests - prev_requests:+,}"
        delta_tpc = f"{tokens_per_credit - prev_tpc:+,.0f}"
    else:
        delta_credits = delta_cost = delta_requests = delta_tpc = None
else:
    delta_credits = delta_cost = delta_requests = delta_tpc = None

c1, c2, c3, c4, c5 = st.columns(5)
c1.metric("Total credits", f"{total_credits:,.4f}", delta=delta_credits)
c2.metric("Estimated cost", f"${total_cost:,.2f}", delta=delta_cost)
c3.metric("Requests", f"{total_requests:,}", delta=delta_requests)
c4.metric("Active users", f"{active_users}")
c5.metric("Tokens / credit", f"{tokens_per_credit:,.0f}", delta=delta_tpc)

daily = (
    filtered.groupby(["usage_date", "source"])
    .agg(credits=("token_credits", "sum"), tokens=("tokens", "sum"), requests=("request_id", "nunique"))
    .reset_index()
)
daily["cost"] = daily["credits"] * price_per_credit
daily["usage_date"] = pd.to_datetime(daily["usage_date"])

tab_credits, tab_cost, tab_users, tab_models, tab_detail, tab_pricing = st.tabs([
    ":material/toll: Credits",
    ":material/payments: Cost",
    ":material/group: Users",
    ":material/model_training: Models",
    ":material/table: Detail",
    ":material/price_check: Pricing Reference",
])

with tab_credits:
    chart = (
        alt.Chart(daily)
        .mark_bar()
        .encode(
            x=alt.X("usage_date:T", title=None),
            y=alt.Y("credits:Q", title="Credits"),
            color=alt.Color("source:N", legend=alt.Legend(orient="bottom")),
            tooltip=[
                alt.Tooltip("usage_date:T", title="Date", format="%Y-%m-%d"),
                alt.Tooltip("source:N", title="Source"),
                alt.Tooltip("credits:Q", title="Credits", format=",.6f"),
            ],
        )
        .properties(height=CHART_HEIGHT)
    )
    st.altair_chart(chart, use_container_width=True)

with tab_cost:
    cost_chart = (
        alt.Chart(daily)
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

with tab_users:
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
                alt.Tooltip("Credits:Q", format=",.6f"),
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
            "Credits": st.column_config.NumberColumn(format="%.6f"),
            "Est. cost ($)": st.column_config.NumberColumn(format="$%.2f"),
            "Tokens": st.column_config.NumberColumn(format="%d"),
            "Requests": st.column_config.NumberColumn(format="%d"),
            "Tokens/Credit": st.column_config.NumberColumn(format="%,.0f"),
        },
    )

with tab_models:
    if granular_filtered.empty:
        st.info("No model-level granular data available.")
    else:
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

        model_summary["derived_input_rate"] = (
            (model_summary["input_credits"] / model_summary["input_tokens"] * 1_000_000)
            .where(model_summary["input_tokens"] > 0, None)
        )
        model_summary["derived_output_rate"] = (
            (model_summary["output_credits"] / model_summary["output_tokens"] * 1_000_000)
            .where(model_summary["output_tokens"] > 0, None)
        )

        st.subheader("Credits by model")
        token_type_data = []
        for _, row in model_summary.iterrows():
            for tt, label in TOKEN_TYPE_LABELS.items():
                cred_val = row.get(f"{tt}_credits", 0) or 0
                tok_val = row.get(f"{tt}_tokens", 0) or 0
                token_type_data.append({
                    "Model": row["model_name"],
                    "Token type": label,
                    "Credits": cred_val,
                    "Tokens": tok_val,
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
                    alt.Tooltip("Credits:Q", format=",.6f"),
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
            "derived_input_rate", "derived_output_rate",
        ]].rename(columns={
            "model_name": "Model", "requests": "Requests",
            "total_tokens": "Total tokens", "total_credits": "Total credits",
            "total_cost": "Est. cost ($)",
            "input_tokens": "Input tokens", "cache_read_tokens": "Cache read tokens",
            "cache_write_tokens": "Cache write tokens", "output_tokens": "Output tokens",
            "derived_input_rate": "Input rate (cr/M tok)", "derived_output_rate": "Output rate (cr/M tok)",
        }).sort_values("Total credits", ascending=False)

        st.dataframe(
            display_models,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Total tokens": st.column_config.NumberColumn(format="%,.0f"),
                "Total credits": st.column_config.NumberColumn(format="%.6f"),
                "Est. cost ($)": st.column_config.NumberColumn(format="$%.2f"),
                "Input tokens": st.column_config.NumberColumn(format="%,.0f"),
                "Cache read tokens": st.column_config.NumberColumn(format="%,.0f"),
                "Cache write tokens": st.column_config.NumberColumn(format="%,.0f"),
                "Output tokens": st.column_config.NumberColumn(format="%,.0f"),
                "Requests": st.column_config.NumberColumn(format="%d"),
                "Input rate (cr/M tok)": st.column_config.NumberColumn(format="%.2f"),
                "Output rate (cr/M tok)": st.column_config.NumberColumn(format="%.2f"),
            },
        )

with tab_detail:
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
            "Credits": st.column_config.NumberColumn(format="%.6f"),
            "Cost": st.column_config.NumberColumn(format="$%.4f"),
            "Tokens": st.column_config.NumberColumn(format="%d"),
            "Tokens/Credit": st.column_config.NumberColumn(format="%,.0f"),
        },
    )

with tab_pricing:
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
        "2. **External Access Integration** — Create a Snowflake "
        "[External Access Integration](https://docs.snowflake.com/en/developer-guide/external-network-access/creating-using-external-network-access) "
        "to allow the app to fetch the PDF without relying on local network egress.\n"
        "3. **Edit the built-in defaults** — Update the `CORTEX_CODE_PRICING` dictionary in "
        "`streamlit_app.py` directly if you prefer a static configuration."
    )

    refresh_col, stage_col, info_col = st.columns([1, 1, 2])
    with refresh_col:
        if not IS_SIS:
            if st.button(":material/refresh: Refresh from PDF", help="Download the latest Snowflake Consumption Table PDF and extract Table 6(g) using Cortex AI_PARSE_DOCUMENT"):
                with st.spinner("Downloading PDF and extracting pricing via Cortex AI_PARSE_DOCUMENT..."):
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
        if st.button(":material/refresh: Refresh from Stage", help=f"Parse CreditConsumptionTable.pdf from @{PRICING_STAGE} using Cortex AI_PARSE_DOCUMENT"):
            with st.spinner("Extracting pricing from staged PDF via Cortex AI_PARSE_DOCUMENT..."):
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
            st.caption(":material/info: Showing built-in pricing defaults. Click refresh to pull latest from Snowflake Consumption Table PDF.")

    st.divider()
    st.subheader("Your observed rates vs. published rates")

    if not granular_filtered.empty:
        observed_rows = []
        for _, row in model_summary.iterrows():
            m = row["model_name"]
            published = active_pricing.get(m, {})
            obs_input = (row["input_credits"] / row["input_tokens"] * 1_000_000) if row["input_tokens"] > 0 else None
            obs_cache_read = (row["cache_read_credits"] / row["cache_read_tokens"] * 1_000_000) if row["cache_read_tokens"] > 0 else None
            obs_cache_write = (row["cache_write_credits"] / row["cache_write_tokens"] * 1_000_000) if row["cache_write_tokens"] > 0 else None
            obs_output = (row["output_credits"] / row["output_tokens"] * 1_000_000) if row["output_tokens"] > 0 else None

            observed_rows.append({
                "Model": m,
                "Input (observed)": obs_input,
                "Input (published)": published.get("input"),
                "Cache Read (observed)": obs_cache_read,
                "Cache Read (published)": published.get("cache_read_input"),
                "Cache Write (observed)": obs_cache_write,
                "Cache Write (published)": published.get("cache_write_input"),
                "Output (observed)": obs_output,
                "Output (published)": published.get("output"),
            })
        obs_df = pd.DataFrame(observed_rows)
        st.dataframe(
            obs_df,
            use_container_width=True,
            hide_index=True,
            column_config={col: st.column_config.NumberColumn(format="%.2f") for col in obs_df.columns if col != "Model"},
        )
    else:
        st.info("No granular usage data available to compare observed vs. published rates.")

    st.caption("Source: [Snowflake Service Consumption Table](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf) — Table 6(g): Cortex Code")

pricing_note = (
    ":material/info: Beginning April 1, 2026, AI credits move to a single list price: "
    "**$2.00/credit (global)** or **$2.20/credit (in-region)**, applied consistently across all customers regardless of edition."
)
st.caption(pricing_note)
st.caption(f":material/info: Data from SNOWFLAKE.ACCOUNT_USAGE views. Available sources: {', '.join(available_sources)}. Price per credit: ${price_per_credit:.2f}")
