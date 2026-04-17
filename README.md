# Snowflake AI Spend Dashboard

A Streamlit dashboard for monitoring credit consumption, cost estimation, and usage analytics across all Snowflake AI products — Cortex Code, AI Functions, Cortex Search, Cortex Analyst, Cortex Agents, and Snowflake Intelligence.

![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)

## Features

### Multi-Product Coverage (8 Tabs)

| Tab | Description |
|-----|-------------|
| **Overview** | Aggregated metrics across all AI products, combined daily stacked bar chart, chargeback table by user |
| **Cortex Code** | Credits, cost, users, model-level breakdown, and detail sub-tabs |
| **AI Functions** | Usage by function, model, and user with daily trend charts |
| **Cortex Search** | Per-service credit consumption (no user attribution) |
| **Cortex Analyst** | Per-user credits and requests with avg cost/request |
| **Agents & SI** | Cortex Agents and Snowflake Intelligence usage by user and resource |
| **Cost Estimator** | Configurable workload forecaster for all products with donut chart breakdown |
| **Pricing Reference** | Built-in credit table with live refresh via `AI_PARSE_DOCUMENT`, observed vs. published rate comparison |

### Key Capabilities

- **Dual pricing model** — Flat AI credit rate ($2.00 global / $2.20 in-region) for Cortex Code, Search, Analyst, and Agents; customer-negotiated credit price for AI Functions (traditional credit billing)
- **Date range filter** — Top-right dropdown: Last 30 days, 90 days, 6 months, 12 months, YTD, or All
- **Sidebar settings** — AI credit tier selector, customer credit price input, Cortex Code source/user filters
- **Chargeback table** — Per-user cost breakdown across all products with CSV export
- **`@st.fragment` performance** — Each tab runs as an independent fragment, avoiding full-script reruns on widget interaction
- **Dual-mode deployment** — Runs both locally (via `snowflake.connector`) and in Streamlit in Snowflake (via `get_active_session()`)
- **Pricing refresh** — Extract Table 6(g) from the Snowflake Consumption Table PDF using `AI_PARSE_DOCUMENT`

---

## Data Sources

This dashboard reads from six `SNOWFLAKE.ACCOUNT_USAGE` views:

| View | Product | User Attribution |
|------|---------|-----------------|
| `CORTEX_CODE_CLI_USAGE_HISTORY` | Cortex Code (CLI) | Yes (via `USER_ID`) |
| `CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY` | Cortex Code (Snowsight) | Yes (via `USER_ID`) |
| `CORTEX_AI_FUNCTIONS_USAGE_HISTORY` | AI Functions | Yes (via `USER_ID`) |
| `CORTEX_SEARCH_SERVING_USAGE_HISTORY` | Cortex Search | No (per-service only) |
| `CORTEX_ANALYST_USAGE_HISTORY` | Cortex Analyst | Yes (via `USERNAME`) |
| `CORTEX_AGENT_USAGE_HISTORY` | Cortex Agents | Yes (via `USER_NAME`) |
| `SNOWFLAKE_INTELLIGENCE_USAGE_HISTORY` | Snowflake Intelligence | Yes (via `USER_NAME`) |

### Access Requirements

These views require one of:

- **ACCOUNTADMIN** role
- A role with **IMPORTED PRIVILEGES** on the `SNOWFLAKE` database:
  ```sql
  GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_role>;
  ```

### Data Latency

`ACCOUNT_USAGE` views have up to **45-minute latency**. Recent usage may not appear immediately.

---

## Prerequisites

- **Snowflake CLI** (`snow`) v3.14.0+
- **Python 3.10+** (for local development)
- **Snowflake connection** configured in `~/.snowflake/connections.toml`
- A role with access to `SNOWFLAKE.ACCOUNT_USAGE` views

### For Streamlit in Snowflake deployment

- A **compute pool** (e.g., `STREAMLIT_COMPUTE_POOL` with `CPU_X64_S`)
- A **PYPI_ACCESS_INTEGRATION** external access integration

---

## Quick Start

### Deploy to Streamlit in Snowflake

```bash
git clone https://github.com/jordandhill/CortexCodeCostTracking.git
cd CortexCodeCostTracking
./deploy.sh
```

The deploy script runs `snow streamlit deploy --replace` using your default connection.

```
./deploy.sh [OPTIONS]

  --connection NAME   Snowflake connection name (default: $SNOWFLAKE_CONNECTION_NAME or "default")
  --help              Show this help message
```

### Run locally

```bash
pip install streamlit pandas altair snowflake-connector-python cryptography
SNOWFLAKE_CONNECTION_NAME=default streamlit run streamlit_app.py
```

---

## Configuration

### Credit Pricing

The sidebar provides two pricing controls:

1. **AI credit pricing tier** — Flat rate for Cortex Code, Search, Analyst, Agents & SI:
   ```python
   CREDIT_PRICE_TIERS = {
       "Global ($2.00) — effective Apr 1, 2026": 2.00,
       "In-region ($2.20) — effective Apr 1, 2026": 2.20,
   }
   ```

2. **Customer cost per credit** — Your negotiated credit price for products billed on traditional credits (e.g., AI Functions). Default: $3.00.

### Model Pricing (Credits per 1M Tokens)

Built-in defaults are in `CORTEX_CODE_PRICING`. These can be refreshed at runtime from the [Snowflake Consumption Table PDF](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf):

1. **Refresh from PDF** — Button in the Pricing Reference tab (local mode only; uses `AI_PARSE_DOCUMENT`)
2. **Refresh from Stage** — Parse a manually uploaded PDF from `@CORTEX_CODE_DASHBOARD.PUBLIC.PRICING_DOCS`
3. **Edit defaults** — Update `CORTEX_CODE_PRICING` in `streamlit_app.py`

> The stage must use `ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')` for `AI_PARSE_DOCUMENT` compatibility.

---

## Project Structure

```
├── streamlit_app.py       # Main application (~1400 lines)
├── snowflake.yml          # Snowflake CLI project definition (definition_version: 2)
├── pyproject.toml         # Python dependencies for SiS
├── deploy.sh              # Deployment script
├── README.md
└── .gitignore
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "No Cortex Code usage data found" | Ensure your role has `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database |
| Only CLI data shown | `CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY` may not yet be available in your account |
| PDF refresh fails with 403 | Snowflake's CDN may block downloads; use the manual stage upload method |
| Connection errors | Verify `~/.snowflake/connections.toml`; set `SNOWFLAKE_CONNECTION_NAME` |
| Data appears stale | `ACCOUNT_USAGE` has up to 45-min latency; cached data refreshes every 10 minutes |
| Container takes long to start | SiS compute pools can take 15-30+ seconds to restart after deploy |

---

## License

MIT
