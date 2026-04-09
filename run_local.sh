#!/usr/bin/env bash
# ─────────────────────────────────────────────────────────────────────────────
# run_local.sh  —  Run the FMCG pipeline locally (no Docker needed)
# Requires: Python 3.10+, Java 11+ (for Spark), pip
# ─────────────────────────────────────────────────────────────────────────────
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV_DIR="$PROJECT_DIR/.venv"

# Colours
GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
log()  { echo -e "${GREEN}[FMCG]${NC} $*"; }
warn() { echo -e "${YELLOW}[WARN]${NC} $*"; }

# ── 0. Set up virtual environment ──────────────────────────────────────────
if [ ! -d "$VENV_DIR" ]; then
    log "Creating virtual environment..."
    python3 -m venv "$VENV_DIR"
fi
source "$VENV_DIR/bin/activate"

log "Installing dependencies..."
pip install -q -r "$PROJECT_DIR/requirements.txt"

# ── Export env vars ────────────────────────────────────────────────────────
export RAW_BASE="$PROJECT_DIR/data_lake/raw"
export CLEAN_BASE="$PROJECT_DIR/data_lake/clean"
export DW_PATH="$PROJECT_DIR/data_lake/fmcg_warehouse.duckdb"

# ── 1. Generate raw data ───────────────────────────────────────────────────
log "Step 1/4 — Generating synthetic raw data..."
python3 "$PROJECT_DIR/scripts/generate_data.py"

# ── 2. Spark clean layer ───────────────────────────────────────────────────
log "Step 2/4 — Running Spark clean layer..."
if command -v spark-submit &> /dev/null; then
    spark-submit \
        --master "local[*]" \
        --driver-memory 2g \
        "$PROJECT_DIR/spark_jobs/clean_layer.py"
else
    warn "spark-submit not found — using pandas fallback for clean layer..."
    python3 "$PROJECT_DIR/scripts/fallback_clean.py"
fi

# ── 3. Build DuckDB warehouse ──────────────────────────────────────────────
log "Step 3/4 — Building DuckDB data warehouse..."
python3 "$PROJECT_DIR/warehouse/build_warehouse.py"

# ── 4. Build data marts ────────────────────────────────────────────────────
log "Step 4/4 — Building data marts..."
python3 "$PROJECT_DIR/data_marts/build_marts.py"

log "✅ Pipeline complete!"
log "   DuckDB database: $DW_PATH"
log ""
log "Explore your data:"
log "  duckdb $DW_PATH"
log "  > SELECT * FROM sales_mart LIMIT 10;"
log "  > SELECT * FROM v_monthly_sales LIMIT 10;"
log "  > SELECT * FROM innovation_mart LIMIT 10;"
