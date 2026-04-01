#!/usr/bin/env bash
set -euo pipefail

CYAN='\033[0;36m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

info()  { echo -e "${CYAN}[INFO]${NC}  $*"; }
ok()    { echo -e "${GREEN}[OK]${NC}    $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC}  $*"; }
err()   { echo -e "${RED}[ERROR]${NC} $*"; }

CONN_NAME="${SNOWFLAKE_CONNECTION_NAME:-${SNOWFLAKE_DEFAULT_CONNECTION_NAME:-default}}"

usage() {
    cat <<EOF
Cortex Code Consumption Dashboard — Deploy to Streamlit in Snowflake

Usage: ./deploy.sh [OPTIONS]

Options:
  --connection NAME   Snowflake connection name (default: \$SNOWFLAKE_CONNECTION_NAME or "default")
  --help              Show this help message

Prerequisites:
  - Snowflake CLI (snow) v3.14.0+
  - A Snowflake connection configured in ~/.snowflake/connections.toml
  - ACCOUNTADMIN or a role with access to SNOWFLAKE.ACCOUNT_USAGE views
  - A compute pool (default: STREAMLIT_COMPUTE_POOL)
  - PYPI_ACCESS_INTEGRATION external access integration

EOF
    exit 0
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --connection) CONN_NAME="$2"; shift 2 ;;
        --help)       usage ;;
        *) err "Unknown option: $1"; usage ;;
    esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

info "Checking Snowflake CLI..."
if ! command -v snow &>/dev/null; then
    err "Snowflake CLI (snow) not found."
    echo "  Install: pip install snowflake-cli  or  brew install snowflake-cli"
    exit 1
fi

SNOW_VERSION=$(snow --version 2>&1 | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
info "Snowflake CLI version: $SNOW_VERSION"

info "Deploying to Streamlit in Snowflake (connection: $CONN_NAME)..."
cd "$SCRIPT_DIR"
snow streamlit deploy --replace --connection "$CONN_NAME"

echo ""
ok "Deployment complete!"
echo ""
info "Open your app in Snowsight under Projects > Streamlit,"
info "or run:  snow streamlit get-url CORTEX_CODE_CONSUMPTION_DASHBOARD --connection $CONN_NAME"
echo ""
