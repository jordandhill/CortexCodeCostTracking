# Cortex Code Consumption Dashboard — Specification

This document fully specifies a Streamlit application that visualizes Snowflake Cortex Code usage, credits, costs, and model-level token breakdowns. An AI coding agent should be able to recreate the app from this spec alone.

---

## 1. Overview

- **Purpose**: Dashboard for monitoring Cortex Code consumption across an entire Snowflake account — credits spent, estimated dollar cost, per-user breakdowns, per-model token analysis, and a pricing reference with live-refresh capability.
- **Runtime**: Must run in **two environments**:
  1. **Streamlit in Snowflake (SIS)** — uses Snowpark `get_active_session()` for queries.
  2. **Local Streamlit** — uses `snowflake.connector` with credentials from `~/.snowflake/connections.toml`.
- **Layout**: Wide mode, single-page app with sidebar filters and 6 content tabs.

---

## 2. Tech Stack & Dependencies

| Package | Min Version | Purpose |
|---|---|---|
| `python` | 3.11 | Runtime (required for `tomllib`) |
| `streamlit` | 1.54.0 | UI framework (with `[snowflake]` extra) |
| `pandas` | 2.0.0 | Data manipulation |
| `altair` | 5.0.0 | Charting |
| `snowflake-connector-python` | 3.3.0 | Local Snowflake connectivity |

Conditional imports (local-only, not needed in SIS):
- `snowflake.connector`
- `cryptography.hazmat.primitives.serialization` (for key-pair auth)
- `pathlib.Path`, `tomllib`

Standard library: `datetime`, `json`, `os`, `re`, `tempfile`, `urllib.request`.

**`pyproject.toml`** should declare these under `[project] dependencies`.

---

## 3. Dual-Runtime Connection Layer

### 3.1 Environment Detection

At module load, attempt to import `snowflake.snowpark.context.get_active_session()`. If it succeeds, set `IS_SIS = True` and store the session. If it fails (any exception), set `IS_SIS = False`.

```python
IS_SIS = False
_snowpark_session = None
try:
    from snowflake.snowpark.context import get_active_session
    _snowpark_session = get_active_session()
    IS_SIS = True
except Exception:
    pass
```

### 3.2 Local Connection (`get_conn`)

Only defined when `IS_SIS is False`. Decorated with `@st.cache_resource`.

1. Read connection name from env vars: `SNOWFLAKE_CONNECTION_NAME` → `SNOWFLAKE_DEFAULT_CONNECTION_NAME` → `"default"`.
2. Parse `~/.snowflake/connections.toml` using `tomllib`.
3. Extract `account`, `user`, `role`, `warehouse` from the named connection.
4. If `private_key_path` is present, load the PEM key via `cryptography`, convert to DER PKCS8, pass as `private_key`.
5. Else if `authenticator` is present, pass it through (supports `externalbrowser`, etc.).
6. Return `snowflake.connector.connect(**kwargs)`.
7. On failure, show `st.error` + `st.info` and `st.stop()`.

Call `get_conn()` once at module level (guarded by `if not IS_SIS`) to eagerly validate the connection before rendering any UI.

### 3.3 Query Helpers

**`run_query(sql: str) -> pd.DataFrame`**: In SIS, uses `_snowpark_session.sql(sql).to_pandas()` and lowercases columns. Locally, uses cursor `.execute()` / `.fetchall()` and builds a DataFrame with lowercased column names.

**`run_scalar(sql: str) -> Any`**: Returns the first column of the first row. In SIS, uses `.collect()[0][0]`. Locally, uses `.fetchone()[0]`.

---

## 4. Constants & Configuration

### 4.1 Data Sources

```python
SOURCES = {
    "CLI": "CORTEX_CODE_CLI_USAGE_HISTORY",
    "Snowsight": "CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY",
}
```

Both are views in `SNOWFLAKE.ACCOUNT_USAGE`.

### 4.2 Time Range Options

```python
TIME_RANGES = ["1W", "1M", "3M", "6M", "YTD", "All"]
```

### 4.3 Credit Pricing Tiers

```python
CREDIT_PRICE_TIERS = {
    "Global ($2.00) — effective Apr 1, 2026": 2.00,
    "In-region ($2.20) — effective Apr 1, 2026": 2.20,
}
```

### 4.4 Default Model Pricing (Credits per 1M Tokens)

Hardcoded fallback pricing for Table 6(g) of the Snowflake Consumption Table:

```python
CORTEX_CODE_PRICING = {
    "claude-4-sonnet":    {"input": 1.50, "cache_read_input": 0.15, "cache_write_input": 1.88, "output": 7.50},
    "claude-opus-4-5":    {"input": 2.75, "cache_read_input": 0.28, "cache_write_input": 3.44, "output": 13.75},
    "claude-opus-4-6":    {"input": 2.75, "cache_read_input": 0.28, "cache_write_input": 3.44, "output": 13.75},
    "claude-sonnet-4-5":  {"input": 1.65, "cache_read_input": 0.17, "cache_write_input": 2.07, "output": 8.25},
    "claude-sonnet-4-6":  {"input": 1.65, "cache_read_input": 0.17, "cache_write_input": 2.07, "output": 8.25},
    "openai-gpt-5.2":     {"input": 0.97, "cache_read_input": 0.10, "cache_write_input": 0.0,  "output": 7.70},
    "openai-gpt-5.44":    {"input": 1.38, "cache_read_input": 0.14, "cache_write_input": 0.0,  "output": 8.25},
}
```

### 4.5 Other Constants

```python
CHART_HEIGHT = 350
PRICING_CHANGE_DATE = date(2026, 4, 1)
CONSUMPTION_TABLE_URL = "https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf"
PRICING_STAGE = "CORTEX_CODE_DASHBOARD.PUBLIC.PRICING_DOCS"
TOKEN_TYPES = ["input", "cache_read", "cache_write", "output"]
TOKEN_TYPE_LABELS = {
    "input": "Input",
    "cache_read": "Cache Read",
    "cache_write": "Cache Write",
    "output": "Output",
}
```

---

## 5. Data Loading

Both loaders use `@st.cache_data(ttl=600)` with a descriptive spinner.

### 5.1 Summary Data (`load_usage_data`)

For each source in `SOURCES`, run:

```sql
SELECT
    u.NAME AS user_name,
    '{label}' AS source,
    DATE_TRUNC('day', c.USAGE_TIME)::DATE AS usage_date,
    c.TOKEN_CREDITS,
    c.TOKENS,
    c.REQUEST_ID
FROM SNOWFLAKE.ACCOUNT_USAGE.{view} c
LEFT JOIN SNOWFLAKE.ACCOUNT_USAGE.USERS u ON c.USER_ID = u.USER_ID
```

- Cast `token_credits` and `tokens` to float.
- Concatenate results from all sources.
- If no data at all, show `st.error` about needing ACCOUNTADMIN or USAGE_VIEWER access and `st.stop()`.

### 5.2 Granular Model Data (`load_granular_data`)

For each source in `SOURCES`, run:

```sql
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
```

- Fill NaN with 0 and cast all token/credit columns to float.
- If no data, return empty DataFrame (non-fatal).

---

## 6. Time Range Filtering

`filter_by_time_range(df, x_col, time_range)` filters a DataFrame by the selected time range:

| Range | Logic |
|---|---|
| `1W` | Last 7 days from max date |
| `1M` | Last 30 days from max date |
| `3M` | Last 90 days from max date |
| `6M` | Last 180 days from max date |
| `YTD` | From January 1 of the max date's year |
| `All` | No filtering |

---

## 7. Page Configuration & Header

```python
st.set_page_config(
    page_title="Cortex Code consumption",
    page_icon=":material/code:",
    layout="wide",
)
```

Header row: `st.title("Cortex Code consumption")` on the left (8-col), a **Reset** button on the right (2-col) that clears `st.cache_data`, `st.session_state`, and reruns.

---

## 8. Sidebar Filters

| Control | Type | Default |
|---|---|---|
| Credit pricing tier | `st.selectbox` | First tier (Global $2.00) |
| Time range | `st.selectbox` | "All" |
| Source | `st.multiselect` | All available sources |
| Users | `st.multiselect` | All users |

If user deselects everything in Source or Users, reset to "all" (treat empty list as no filter).

Apply filters to both the summary and granular DataFrames.

---

## 9. KPI Metrics Row

Five `st.metric` widgets across 5 equal columns:

| Metric | Format | Delta |
|---|---|---|
| Total credits | `,.4f` | Change vs. previous period |
| Estimated cost | `$,.2f` | Change vs. previous period |
| Requests | `,` (integer) | Change vs. previous period |
| Active users | plain integer | No delta |
| Tokens / credit | `,.0f` | Change vs. previous period |

**Period-over-period delta logic** (only when time range is not "All"):
1. Determine the current period's `min_date` and `max_date`.
2. Calculate `span = max_date - min_date` in days.
3. Previous period = `[min_date - span - 1 day, min_date - 1 day]`.
4. Compute the same metrics for the previous period.
5. Delta = current - previous. Format: credits as `+.4f`, cost as `$+,.2f`, requests as `+,` (signed integer with comma grouping), tokens/credit as `+,.0f`.
6. If span is 0, deltas are `None`.

---

## 10. Tab Structure

Six tabs with Material icons:

```python
[
    ":material/toll: Credits",
    ":material/payments: Cost",
    ":material/group: Users",
    ":material/model_training: Models",
    ":material/table: Detail",
    ":material/price_check: Pricing Reference",
]
```

### 10.1 Daily Aggregation (shared by Credits & Cost tabs)

Group filtered summary data by `(usage_date, source)`:
- `credits` = sum of `token_credits`
- `tokens` = sum of `tokens`
- `requests` = nunique of `request_id`
- `cost` = `credits * price_per_credit`

### 10.2 Credits Tab

Stacked bar chart (Altair):
- X: `usage_date:T` (no title)
- Y: `credits:Q` (title: "Credits")
- Color: `source:N` (legend at bottom)
- Tooltip: date (`%Y-%m-%d`), source, credits (`,.6f`)
- Height: `CHART_HEIGHT` (350)
- Full container width.

### 10.3 Cost Tab

Same structure as Credits tab but:
- Y: `cost:Q` (title: "Estimated cost ($)")
- Tooltip cost format: `$,.2f`

### 10.4 Users Tab

**Bar chart**: Users on X (sorted descending by cost), estimated cost on Y, colored per user (no legend), rounded top corners (`cornerRadiusTopLeft=4, cornerRadiusTopRight=4`).

Tooltips: User, Credits (`,.6f`), Est. cost (`$,.2f`), Requests (`,`), Tokens/Credit (`,.0f`).

**Data table** below the chart with columns:
| Column | Format |
|---|---|
| User | plain |
| Credits | `%.6f` |
| Est. cost ($) | `$%.2f` |
| Tokens | `%d` |
| Requests | `%d` |
| Tokens/Credit | `%,.0f` |

### 10.5 Models Tab

Only renders if the **filtered** granular data (`granular_filtered`) is non-empty. Otherwise shows `st.info`.

**Important**: The `model_summary` DataFrame computed in this tab is also used by the Pricing Reference tab's "observed vs. published rates" section. Since Python `with` blocks do not create new variable scopes, `model_summary` remains accessible outside the `with tab_models:` block. A reimplementation must ensure this variable is available to the Pricing tab.

**Aggregation**: Group granular data by `model_name`. Sum all 8 token/credit columns, nunique `request_id`. Compute `total_tokens`, `total_credits`, `total_cost`, `derived_input_rate`, `derived_output_rate` (credits / tokens * 1M, where tokens > 0).

**Stacked bar chart** ("Credits by model"):
- Reshape data: for each model and each of the 4 token types, create a row with Model, Token type, Credits, Tokens.
- X: Model (sorted descending), Y: Credits, Color: Token type (domain = `["Input", "Cache Read", "Cache Write", "Output"]`), legend at bottom.

**Model summary table** with columns:
| Column | Format |
|---|---|
| Model | plain |
| Requests | `%d` |
| Total tokens | `%,.0f` |
| Total credits | `%.6f` |
| Est. cost ($) | `$%.2f` |
| Input tokens | `%,.0f` |
| Cache read tokens | `%,.0f` |
| Cache write tokens | `%,.0f` |
| Output tokens | `%,.0f` |
| Input rate (cr/M tok) | `%.2f` |
| Output rate (cr/M tok) | `%.2f` |

### 10.6 Detail Tab

Raw row-level data table:

| Column | Source | Format |
|---|---|---|
| Date | `usage_date` | default |
| Source | `source` | default |
| User | `user_name` | default |
| Credits | `token_credits` | `%.6f` |
| Cost | computed (`Credits * price_per_credit`) | `$%.4f` |
| Tokens | `tokens` | `%d` |
| Tokens/Credit | computed | `%,.0f` |
| Request ID | `request_id` | default |

Sorted by Date descending. Table height: 500px.

### 10.7 Pricing Reference Tab

**Section 1: Published pricing table**
- Header: "Snowflake AI Features Credit Table"
- Caption: "Table 6(g): Cortex Code — Credits per 1M tokens by model and token type"
- Data source: `st.session_state.get("refreshed_pricing", CORTEX_CODE_PRICING)`
- Columns: Model, Input (`%.2f`), Cache Read (`%.2f`), Cache Write (`%.2f`), Output (`%.2f`)
- Each number column has a help tooltip explaining it.

**Section 2: Disclaimer**
`st.warning` noting pricing is subject to change, with link to the official PDF.

**Section 3: Refresh instructions**
`st.markdown` block explaining three update methods:
1. Manual download + stage upload (with SQL `PUT` example and Snowsight UI instructions).
2. External Access Integration.
3. Edit the built-in `CORTEX_CODE_PRICING` dict.

**Section 4: Refresh buttons** (3-column layout)
- **"Refresh from PDF"** (local only, hidden in SIS): Downloads PDF from `CONSUMPTION_TABLE_URL`, uploads to `PRICING_STAGE`, parses with `AI_PARSE_DOCUMENT`.
- **"Refresh from Stage"** (both environments): Parses the already-staged PDF with `AI_PARSE_DOCUMENT`.
- **Info column**: Shows whether pricing was refreshed or is using defaults.

**Section 5: Observed vs. Published rates**
If `granular_filtered` (the post-filter granular data) is non-empty, show a comparison table iterating over `model_summary` (from the Models tab). For each model and each token type, show both the observed rate (derived from actual usage: `credits / tokens * 1M`) and the published rate (from the active pricing dict). Format: `%.2f`. If `granular_filtered` is empty (either no granular data at all or all filtered away), show `st.info` instead.

---

## 11. Pricing Refresh Logic

### 11.1 PDF Parsing (`refresh_pricing_from_pdf` and `refresh_pricing_from_stage`)

Both functions share the same parsing logic after obtaining the raw JSON:

1. Call `AI_PARSE_DOCUMENT(TO_FILE('@{PRICING_STAGE}', 'CreditConsumptionTable.pdf'), {'mode': 'LAYOUT', 'page_split': true})::VARCHAR`.
2. Parse the JSON result.
3. Iterate over `pages`. Find the page containing both `"6(g)"` and `"cortex code"` (case-insensitive).
4. Parse pipe-delimited table rows:
   - Skip blank lines, `---` separators, lines starting with "Model" or "Snowflake".
   - Require at least 5 pipe-separated parts.
   - Model name must start with a lowercase letter (`^[a-z]`).
   - Extract: `parts[0]` = model, `parts[1]` = input, `parts[2]` = output, `parts[3]` = cache_write (or 0 if `"-"`), `parts[4]` = cache_read.
5. Store in session state as `refreshed_pricing`.
6. Break after first matching page.

### 11.2 `refresh_pricing_from_pdf` (Local Only)

Before parsing:
1. Raise `RuntimeError` if `IS_SIS` is True.
2. Download the PDF from `CONSUMPTION_TABLE_URL` to a temp directory. Use a custom `User-Agent` header to avoid being blocked:
   ```python
   req = urllib.request.Request(url, headers={
       "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36"
   })
   ```
3. Create the database, schema, and stage if they don't exist.
4. `PUT` the file to the stage.

### 11.3 `refresh_pricing_from_stage` (Both Environments)

Directly calls `AI_PARSE_DOCUMENT` on the staged file (assumes it already exists).

---

## 12. Footer Captions

Two `st.caption` lines at the bottom of the page (outside all tabs):

1. Pricing note about the April 1, 2026 credit price change ($2.00 global / $2.20 in-region).
2. Data source note showing which `ACCOUNT_USAGE` views are available and the current price per credit.

---

## 13. Deployment

### 13.1 `snowflake.yml`

```yaml
definition_version: 2
entities:
  cortex_code_dashboard:
    type: streamlit
    identifier:
      name: CORTEX_CODE_CONSUMPTION_DASHBOARD
      database: CORTEX_CODE_DASHBOARD
      schema: PUBLIC
    query_warehouse: COMPUTE_G2_M
    runtime_name: SYSTEM$ST_CONTAINER_RUNTIME_PY3_11
    compute_pool: STREAMLIT_COMPUTE_POOL
    external_access_integrations:
      - PYPI_ACCESS_INTEGRATION
    main_file: streamlit_app.py
    artifacts:
      - streamlit_app.py
      - pyproject.toml
```

### 13.2 Deploy Command

```bash
snow streamlit deploy --replace
```

### 13.3 Prerequisites

- A Snowflake account with Cortex Code usage data.
- ACCOUNTADMIN or a role with access to `SNOWFLAKE.ACCOUNT_USAGE` views.
- A warehouse (e.g., `COMPUTE_G2_M`) and a compute pool (e.g., `STREAMLIT_COMPUTE_POOL`).
- External access integration `PYPI_ACCESS_INTEGRATION` for installing Python packages from PyPI in SIS.
- For pricing refresh: the stage `CORTEX_CODE_DASHBOARD.PUBLIC.PRICING_DOCS` with encryption type `SNOWFLAKE_SSE`, and access to the `AI_PARSE_DOCUMENT` Cortex function.

---

## 14. File Structure

```
project-root/
├── streamlit_app.py   # The entire application (single file)
├── pyproject.toml     # Python dependencies
├── snowflake.yml      # Snow CLI deployment config
└── SPEC.md            # This file
```

---

## 15. Key Behaviors & Edge Cases

- If neither ACCOUNT_USAGE view returns data, show an error and stop.
- If granular data is unavailable (LATERAL FLATTEN fails or returns nothing), the Models tab shows an info message and the pricing comparison section is skipped. This is non-fatal.
- Silently skip individual source queries that fail (e.g., if the Snowsight view doesn't exist yet).
- Empty multi-select filters are treated as "select all" (no filter applied).
- Caching TTL is 600 seconds (10 minutes) for both data loaders. The Reset button clears all caches and session state.
- All column names are lowercased after query execution for consistency between SIS and local runtimes.
