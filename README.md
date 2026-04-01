# Cortex Code Consumption Dashboard

A Streamlit dashboard for monitoring [Snowflake Cortex Code](https://docs.snowflake.com/en/user-guide/cortex-code) credit consumption, cost estimation, and per-model token usage across your account.

![Dashboard](https://img.shields.io/badge/Streamlit-FF4B4B?logo=streamlit&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?logo=snowflake&logoColor=white)

## Features

- **Credit tracking** — Daily credit consumption with stacked bar charts by source (CLI / Snowsight)
- **Cost estimation** — Estimated dollar cost using configurable credit pricing tiers (Global $2.00 / In-region $2.20)
- **User breakdown** — Per-user credit, token, and request metrics
- **Model-level analysis** — Granular token and credit breakdown by model and token type (input, output, cache read, cache write)
- **Observed vs. published rates** — Compare your actual per-model credit rates against Snowflake's published consumption table
- **Pricing reference** — Built-in pricing table with optional live refresh from the Snowflake Consumption Table PDF via `AI_PARSE_DOCUMENT`
- **Time range & source filters** — Sidebar controls for 1W, 1M, 3M, 6M, YTD, or All; filter by source and user
- **Period-over-period deltas** — Metric cards show changes compared to the previous equivalent period

---

## Data Sources

This dashboard reads from two `SNOWFLAKE.ACCOUNT_USAGE` views:

### `CORTEX_CODE_CLI_USAGE_HISTORY`

Records usage from the **Cortex Code CLI** (the `cortex` command-line tool). Each row represents a single request and includes:

| Column | Description |
|--------|-------------|
| `USER_ID` | The Snowflake user who made the request |
| `USAGE_TIME` | Timestamp of the request |
| `REQUEST_ID` | Unique request identifier |
| `TOKEN_CREDITS` | Total credits consumed by this request |
| `TOKENS` | Total tokens consumed |
| `TOKENS_GRANULAR` | VARIANT — per-model token breakdown by type (`input`, `output`, `cache_read_input`, `cache_write_input`) |
| `CREDITS_GRANULAR` | VARIANT — per-model credit breakdown by type (same keys as `TOKENS_GRANULAR`) |

### `CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY`

Records usage from **Cortex Code in Snowsight** (the browser-based IDE). Schema is identical to `CORTEX_CODE_CLI_USAGE_HISTORY`.

> **Note:** `CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY` may not yet be available in all accounts. The dashboard gracefully handles its absence — if the view doesn't exist, only CLI data is shown.

### Access Requirements

These views are in the `SNOWFLAKE.ACCOUNT_USAGE` schema, which requires one of:

- **ACCOUNTADMIN** role
- A role with the **IMPORTED PRIVILEGES** grant on the `SNOWFLAKE` database:
  ```sql
  GRANT IMPORTED PRIVILEGES ON DATABASE SNOWFLAKE TO ROLE <your_role>;
  ```

### Data Latency

`ACCOUNT_USAGE` views have up to **45-minute latency** from the time of the actual usage event. Recent requests may not appear immediately.

---

## Prerequisites

- **Python 3.10+**
- **Snowflake connection** configured in `~/.snowflake/connections.toml`
- A role with access to `SNOWFLAKE.ACCOUNT_USAGE` views (see above)

### Example `~/.snowflake/connections.toml`

```toml
[default]
account = "myorg-myaccount"
user = "MYUSER"
role = "ACCOUNTADMIN"
warehouse = "COMPUTE_WH"
authenticator = "externalbrowser"    # or use private_key_path
# private_key_path = "/path/to/rsa_key.p8"
```

---

## Quick Start

### 1. Clone the repository

```bash
git clone https://github.com/jordandhill/CortexCodeCostTracking.git
cd CortexCodeCostTracking
```

### 2. Run the deploy script

```bash
./deploy.sh
```

This will:
- Create a `.venv` virtual environment and install Python dependencies
- Create the `CORTEX_CODE_DASHBOARD` database, schema, and pricing stage in Snowflake
- Verify access to the `ACCOUNT_USAGE` views
- Print the command to start the app

### Deploy script options

```
./deploy.sh [OPTIONS]

  --connection NAME   Snowflake connection name (default: $SNOWFLAKE_CONNECTION_NAME or "default")
  --database NAME     Database for the pricing stage (default: CORTEX_CODE_DASHBOARD)
  --port PORT         Streamlit port (default: 8501)
  --skip-snowflake    Skip Snowflake object creation
  --help              Show help
```

### 3. Start the dashboard

```bash
source .venv/bin/activate
SNOWFLAKE_CONNECTION_NAME=default streamlit run streamlit_app.py
```

Or specify a different connection:

```bash
SNOWFLAKE_CONNECTION_NAME=my_connection streamlit run streamlit_app.py
```

---

## Configuration

### Credit Pricing Tiers

The sidebar dropdown lets you select a credit pricing tier. The available tiers are defined in `streamlit_app.py`:

```python
CREDIT_PRICE_TIERS = {
    "Global ($2.00) — effective Apr 1, 2026": 2.00,
    "In-region ($2.20) — effective Apr 1, 2026": 2.20,
}
```

Edit this dictionary to add or modify tiers for your contract.

### Model Pricing (Credits per 1M Tokens)

Built-in pricing defaults are in the `CORTEX_CODE_PRICING` dictionary. These can be refreshed at runtime from the [Snowflake Service Consumption Table PDF](https://www.snowflake.com/legal-files/CreditConsumptionTable.pdf) using the **Refresh from PDF** button in the Pricing Reference tab.

#### Updating pricing

1. **Refresh from PDF** — Click the button in the Pricing Reference tab. Uses `AI_PARSE_DOCUMENT` to extract Table 6(g) from the Snowflake Consumption Table PDF.

2. **Manual upload** — Download the PDF yourself and upload it to the Snowflake stage:
   ```sql
   PUT file:///path/to/CreditConsumptionTable.pdf
       @CORTEX_CODE_DASHBOARD.PUBLIC.PRICING_DOCS
       AUTO_COMPRESS=FALSE OVERWRITE=TRUE;
   ```
   You can also upload via the **Snowsight UI**: navigate to **Data > Databases > CORTEX_CODE_DASHBOARD > PUBLIC > Stages > PRICING_DOCS** and click **+ Files**.

3. **Edit defaults** — Update `CORTEX_CODE_PRICING` in `streamlit_app.py` directly.

> **Stage location:** `@CORTEX_CODE_DASHBOARD.PUBLIC.PRICING_DOCS` — configurable via the `PRICING_STAGE` constant. The stage must use `ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')` for `AI_PARSE_DOCUMENT` compatibility.

---

## Project Structure

```
├── streamlit_app.py       # Main application
├── deploy.sh              # Deployment script
├── .gitignore
├── .streamlit/
│   └── secrets.toml       # (git-ignored) Optional Streamlit secrets
└── README.md
```

---

## Troubleshooting

| Issue | Solution |
|-------|----------|
| "No Cortex Code usage data found" | Ensure your role has access to `SNOWFLAKE.ACCOUNT_USAGE` views. Grant `IMPORTED PRIVILEGES` on the `SNOWFLAKE` database. |
| Only CLI data shown, no Snowsight data | `CORTEX_CODE_SNOWSIGHT_USAGE_HISTORY` may not yet be available in your account. |
| PDF refresh fails with 403 | Snowflake's CDN may block automated downloads. Use the manual upload method instead. |
| PDF refresh fails with network error | Outbound internet access may be restricted. Upload the PDF manually to the stage or create an External Access Integration. |
| Connection errors | Verify your `~/.snowflake/connections.toml` configuration. Set `SNOWFLAKE_CONNECTION_NAME` to match your connection entry. |
| Data appears stale | `ACCOUNT_USAGE` views have up to 45-minute latency. Click **Reset** to clear the 10-minute app cache. |

---

## License

MIT
