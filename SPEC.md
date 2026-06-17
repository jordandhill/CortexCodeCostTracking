# Snowflake AI Spend Dashboard — Specification

This document specifies a Streamlit application that visualizes Snowflake AI consumption — credits, estimated dollar cost, per-user chargeback, per-model token breakdowns, a workload cost estimator, and a pricing reference with live-refresh capability — across all Snowflake AI products. An AI coding agent should be able to recreate the app from this spec alone.

---

## 1. Overview

- **Purpose**: Account-wide dashboard for monitoring Snowflake AI spend across Cortex Code (CLI, Snowsight, Desktop), AI Functions, Cortex Search (serving + indexing), Cortex Analyst, Cortex Agents, Snowflake Intelligence, Document Processing, Fine-Tuning, Provisioned Throughput, Cortex REST API, and AISQL.
- **Runtime**: Must run in **two environments**:
  1. **Streamlit in Snowflake (SiS)** — uses Snowpark `get_active_session()` for queries.
  2. **Local Streamlit** — uses `snowflake.connector` with credentials from `~/.snowflake/connections.toml`.
- **Layout**: Wide mode, single-page app with a top-right date range selector, sidebar settings/filters, and 13 content tabs.

---

## 2. Tech Stack & Dependencies

| Package | Min Version | Purpose |
|---|---|---|
| `python` | 3.11 | Runtime (required for `tomllib`) |
| `streamlit[snowflake]` | 1.54.0 | UI framework |
| `pandas` | 2.0.0 | Data manipulation |
| `altair` | 5.0.0 | Charting |
| `snowflake-connector-python` | 3.3.0 | Local Snowflake connectivity |

Conditional imports (local-only, not needed in SiS): `snowflake.connector`, `cryptography.hazmat.primitives.serialization`, `pathlib.Path`, `tomllib`.

Standard library: `datetime`, `json`, `os`, `re`, `tempfile`, `urllib.request`.

---

## 3. Dual-Runtime Connection Layer

### 3.1 Environment Detection

At module load, attempt `from snowflake.snowpark.context import get_active_session`. On success set `IS_SIS = True` and store the session; on any exception set `IS_SIS = False`.

### 3.2 Local Connection (`get_conn`)

Only defined when `IS_SIS is False`; decorated with `@st.cache_resource`. Reads the connection name from `SNOWFLAKE_CONNECTION_NAME` → `SNOWFLAKE_DEFAULT_CONNECTION_NAME` → `"default"`, parses `~/.snowflake/connections.toml` via `tomllib`, and connects using key-pair auth (`private_key_path` → DER PKCS8) or `authenticator`. On failure, shows `st.error`/`st.info` and `st.stop()`. Called once at module level (guarded by `if not IS_SIS`).

### 3.3 Query Helpers

- **`run_query(sql) -> pd.DataFrame`**: SiS uses `_snowpark_session.sql(sql).to_pandas()`; local uses cursor `.execute()`/`.fetchall()`. Column names are lowercased in both paths.
- **`run_scalar(sql)`**: Returns the first column of the first row.

---

## 4. Constants & Configuration

### 4.1 Cortex Code Sources

```python
SOURCES = {
    "CLI": "CORTEX_CODE_CLI_USAGE_HISTORY",
    "Snowsight": "CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY",
    "Desktop": "CORTEX_CODE_DESKTOP_USAGE_HISTORY",
}
```

All three are views in `SNOWFLAKE.ACCOUNT_USAGE` with identical schemas, covering distinct (mutually exclusive) Cortex Code entry points. Each exposes `USER_NAME` directly (no join to `USERS` required).

### 4.2 Time Range Options

```python
TIME_RANGES = ["Last 30 days", "Last 90 days", "Last 6 months", "Last 12 months", "YTD", "All"]
```

Rendered as a top-right `st.selectbox` (default index 0 = "Last 30 days").

### 4.3 Credit Pricing Tiers

```python
CREDIT_PRICE_TIERS = {
    "Global ($2.00) — effective Apr 1, 2026": 2.00,
    "In-region ($2.20) — effective Apr 1, 2026": 2.20,
}
```

The selected tier provides `price_per_credit` (the flat AI credit rate). A separate sidebar `customer_credit_price` (default $3.00) applies to products billed on traditional credits (AI Functions).

### 4.4 Model & Rate Pricing

- `CORTEX_CODE_PRICING` — Table 6(g) defaults, credits per 1M tokens, keyed by model with `input` / `cache_read_input` / `cache_write_input` / `output`. Verified accurate against observed account rates.
- `AI_COMPLETE_PRICING` — AI_COMPLETE model rates (estimates) used by the Cost Estimator.
- `CORTEX_SEARCH_RATE_PER_1K = 0.25`, `CORTEX_ANALYST_RATE_PER_REQUEST = 0.06` — estimator defaults (published rates, subject to change).

### 4.5 Other Constants

```python
CHART_HEIGHT = 350
CONSUMPTION_TABLE_URL = "https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf"
PRICING_STAGE = "CORTEX_CODE_DASHBOARD.PUBLIC.PRICING_DOCS"
TOKEN_TYPE_LABELS = {"input": "Input", "cache_read": "Cache Read", "cache_write": "Cache Write", "output": "Output"}
AI_FUNCTION_TYPES = [...]  # estimator function-type options
```

---

## 5. Data Sources & Loaders

Every loader uses `@st.cache_data(ttl=600)` with a descriptive spinner and wraps its query in `try/except`, returning an empty DataFrame on failure (so a missing/unavailable view degrades gracefully).

| Loader | View(s) | Key columns | Attribution |
|---|---|---|---|
| `load_usage_data` | `SOURCES` (CLI/Snowsight/Desktop) | `USER_NAME`, `USAGE_TIME`, `TOKEN_CREDITS`, `TOKENS`, `REQUEST_ID` | `USER_NAME` |
| `load_granular_data` | `SOURCES` | `TOKENS_GRANULAR`/`CREDITS_GRANULAR` (OBJECT) via `LATERAL FLATTEN` | `USER_NAME` |
| `load_ai_functions_data` | `CORTEX_AI_FUNCTIONS_USAGE_HISTORY` | `FUNCTION_NAME`, `MODEL_NAME`, `CREDITS`, `USER_ID`→`USERS` | user |
| `load_cortex_search_data` | `CORTEX_SEARCH_SERVING_USAGE_HISTORY` | `DATABASE_NAME`, `SCHEMA_NAME`, `SERVICE_NAME`, `CREDITS` | per-service |
| `load_cortex_analyst_data` | `CORTEX_ANALYST_USAGE_HISTORY` | `USERNAME`, `CREDITS`, `REQUEST_COUNT` | user |
| `load_agents_data` | `CORTEX_AGENT_USAGE_HISTORY` + `SNOWFLAKE_INTELLIGENCE_USAGE_HISTORY` | `USER_NAME`, `AGENT_NAME`/`SNOWFLAKE_INTELLIGENCE_NAME`, `TOKEN_CREDITS`, `TOKENS` | user |
| `load_doc_processing_data` | `CORTEX_DOCUMENT_PROCESSING_USAGE_HISTORY` | `FUNCTION_NAME`, `MODEL_NAME`, `CREDITS_USED`, `DOCUMENT_COUNT`, `PAGE_COUNT` | per-service |
| `load_fine_tuning_data` | `CORTEX_FINE_TUNING_USAGE_HISTORY` | `MODEL_NAME`, `TOKEN_CREDITS`, `TOKENS` (uses `START_TIME`) | per-service |
| `load_rest_api_data` | `CORTEX_REST_API_USAGE_HISTORY` | `MODEL_NAME`, `INFERENCE_REGION`, `TOKENS`, `USER_ID`→`USERS` | user (tokens only, **no credits**) |
| `load_provisioned_throughput_data` | `CORTEX_PROVISIONED_THROUGHPUT_USAGE_HISTORY` | `AI_SERVICE`, `MODEL_NAME`, `PTU_CREDITS`, `PTU_COUNT` (uses `INTERVAL_START_TIME`) | per-service |
| `load_search_indexing_data` | `CORTEX_SEARCH_DAILY_USAGE_HISTORY` | `CONSUMPTION_TYPE`, `DATABASE_NAME`, `SCHEMA_NAME`, `SERVICE_NAME`, `CREDITS` (uses `USAGE_DATE`) | per-service |
| `load_aisql_data` | `CORTEX_AISQL_USAGE_HISTORY` | `FUNCTION_NAME`, `MODEL_NAME`, `TOKEN_CREDITS`, `TOKENS` (uses `USAGE_TIME`) | standalone |

**Schema notes (verified live):** credit column names differ by view (`TOKEN_CREDITS`, `CREDITS`, `CREDITS_USED`, `PTU_CREDITS`); time columns differ (`USAGE_TIME`, `START_TIME`, `INTERVAL_START_TIME`, `USAGE_DATE`). Code-view granular columns are `OBJECT`s keyed by model name whose values contain `input`/`cache_read_input`/`cache_write_input`/`output`.

---

## 6. Time Range Filtering

`filter_by_time_range(df, x_col, time_range)` filters by max-date offset: 30/90/180/365 days for the four "Last …" options, Jan 1 of the max year for `YTD`, and no filter for `All`.

---

## 7. Page Configuration & Header

```python
st.set_page_config(page_title="Snowflake AI Spend", page_icon=":material/monitoring:", layout="wide")
```

Header row: `st.title("Snowflake AI Spend")` on the left; the **Date range** `st.selectbox` on the right (label hidden).

---

## 8. Sidebar Settings & Filters

| Control | Type | Default |
|---|---|---|
| AI credit pricing tier | `st.selectbox` | Global ($2.00) |
| Customer cost per credit ($) | `st.number_input` | 3.00 |
| Cortex Code Source filter | `st.multiselect` | All available sources |
| Cortex Code Users filter | `st.multiselect` | All users |

Empty multiselects are treated as "select all". Source/User filters apply to the Cortex Code summary and granular DataFrames; the date range applies to every product.

---

## 9. Tab Structure (13 tabs)

`Overview`, `Cortex Code`, `AI Functions`, `Cortex Search`, `Cortex Analyst`, `Agents & SI`, `Doc Processing`, `Fine-Tuning`, `REST API`, `Prov. Throughput`, `AISQL`, `Cost Estimator`, `Pricing Reference`. Each tab body is an `@st.fragment` function to avoid full-script reruns on widget interaction.

### 9.1 Overview

- Two rows of five `st.metric` tiles (credits + estimated cost): Cortex Code, AI Functions, Cortex Search, Cortex Analyst, Agents & SI / Doc Processing, Fine-Tuning, Search Indexing, Prov. Throughput, REST API (tokens-only).
- **Grand total** caption summing all credit-bearing products. **Excludes** REST API (no credits) and AISQL (avoids double-counting with AI Functions); a second caption states this.
- Combined daily stacked bar chart (`product` color) across all credit-bearing products.
- **Chargeback table** — per-user credits/cost across user-attributed products only (Cortex Code, AI Functions, Cortex Analyst, Agents & SI), with CSV export. Per-service products (Search, Doc Processing, Fine-Tuning, Prov. Throughput) are not in chargeback.
- Cortex Search serving spend-by-service summary.

### 9.2 Cortex Code

Sub-tabs: Credits, Cost, Users, Models, Detail. Credits/Cost are daily stacked bars by `source` (now CLI/Snowsight/Desktop). Models uses `granular_filtered`/`model_summary` (token-type stacked bar + summary table). `model_summary` is reused by the Pricing tab's observed-vs-published comparison.

### 9.3–9.6 AI Functions / Cortex Search / Cortex Analyst / Agents & SI

As previously specified: daily charts plus by-function/model/user/service/resource summary tables. Cortex Search additionally renders an **Indexing & serving by consumption type** section from `CORTEX_SEARCH_DAILY_USAGE_HISTORY` (rendered whenever indexing data exists, independent of serving data).

### 9.7 Doc Processing / Fine-Tuning / Prov. Throughput / AISQL (new)

Each shows metric tiles, a daily credits chart, and a summary table. REST API shows **token volume only** (no dollar cost) with an explanatory `st.info`. AISQL displays a caption noting it is excluded from the grand total.

### 9.8 Cost Estimator

Configurable per-product workload forecaster (developers/requests/tokens/calls), forecast table, KPIs, donut chart, and CSV download. AI Functions use `customer_credit_price`; other products use the AI tier `price_per_credit`.

### 9.9 Pricing Reference

Published Table 6(g) pricing table (from `st.session_state["refreshed_pricing"]` or `CORTEX_CODE_PRICING`), a "subject to change" disclaimer, refresh instructions, **Refresh from PDF** (local only) / **Refresh from Stage** buttons using `AI_PARSE_DOCUMENT`, and an observed-vs-published rate comparison driven by `model_summary`.

---

## 10. Pricing Refresh Logic

`refresh_pricing_from_pdf` (local only) downloads the Consumption Table PDF with a browser `User-Agent`, creates DB/schema/stage (`SNOWFLAKE_SSE` encryption), `PUT`s the file, then parses. `refresh_pricing_from_stage` parses the already-staged file. Both call `AI_PARSE_DOCUMENT(TO_FILE('@{PRICING_STAGE}', 'CreditConsumptionTable.pdf'), {'mode': 'LAYOUT', 'page_split': true})`, locate the page containing `6(g)` + `cortex code`, parse pipe-delimited rows (model must start lowercase; columns: model, input, output, cache_write|"-", cache_read), and store results in `st.session_state["refreshed_pricing"]`.

---

## 11. Footer

A single `st.caption` noting the `ACCOUNT_USAGE` source, the available Cortex Code sources, the active AI/customer credit prices, the billing model, and the full list of covered products.

---

## 12. Deployment

### 12.1 `snowflake.yml`

```yaml
definition_version: 2
entities:
  cortex_code_dashboard:
    type: streamlit
    identifier:
      name: CORTEX_CODE_CONSUMPTION_DASHBOARD
      database: CORTEX_CODE_DASHBOARD
      schema: PUBLIC
    query_warehouse: COMPUTE_G2_S
    runtime_name: SYSTEM$ST_CONTAINER_RUNTIME_PY3_11
    compute_pool: STREAMLIT_COMPUTE_POOL
    external_access_integrations:
      - PYPI_ACCESS_INTEGRATION
    main_file: streamlit_app.py
    artifacts:
      - streamlit_app.py
      - pyproject.toml
```

### 12.2 Deploy

```bash
snow streamlit deploy --replace
```

### 12.3 Prerequisites

- ACCOUNTADMIN, or a role with `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database, to read `ACCOUNT_USAGE`.
- A query warehouse and a compute pool for SiS.
- `PYPI_ACCESS_INTEGRATION` for PyPI installs in SiS.
- For pricing refresh: the `PRICING_STAGE` with `SNOWFLAKE_SSE` encryption and access to `AI_PARSE_DOCUMENT`.

---

## 13. File Structure

```
project-root/
├── streamlit_app.py   # The entire application (single file)
├── pyproject.toml     # Python dependencies
├── snowflake.yml      # Snow CLI deployment config
├── README.md
└── SPEC.md            # This file
```

---

## 14. Key Behaviors & Edge Cases

- `ACCOUNT_USAGE` has up to 45-minute latency; loaders cache for 600 seconds.
- If no Cortex Code data is returned from any source, show an error and `st.stop()`.
- Every other loader fails silently (empty DataFrame) so a missing/unavailable view simply hides that product's tab content.
- Cortex Code views expose `USER_NAME` directly — no `USERS` join.
- REST API has no credits column → token volume only.
- AISQL is excluded from the Overview grand total to avoid double-counting with AI Functions.
- Empty multiselect filters are treated as "select all".
- All column names are lowercased after query execution for SiS/local consistency.
