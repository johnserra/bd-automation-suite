#!/usr/bin/env bash
# =============================================================================
# run_weekly.sh — Weekly BD automation run
# =============================================================================
# Intended cron schedule (run Sunday night so results are ready Monday morning):
#   0 23 * * 0 /home/johnserra/Projects/business-development/scripts/run_weekly.sh >> /var/log/bd-weekly.log 2>&1
# =============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

cd "$PROJECT_ROOT"

# Load environment variables
if [ -f .env ]; then
    set -a
    source .env
    set +a
fi

echo "=== BD Weekly Run — $(date '+%Y-%m-%d %H:%M:%S') ==="

# --- Module 1: Prospect Research ---
# Find new leads for each stream with enabled data sources
echo "[1/3] Running Prospect Research — stream_a..."
uv run python -m modules.prospect_research.main --stream stream_a
echo "      Done."

echo "[2/3] Running Prospect Research — stream_c..."
uv run python -m modules.prospect_research.main --stream stream_c
echo "      Done."

# --- Module 7: Pipeline Reporter ---
# Generate weekly markdown report
echo "[3/3] Generating Weekly Pipeline Report..."
uv run python -m modules.pipeline_reporter.main --period weekly
echo "      Done."

echo "=== Weekly run complete — $(date '+%H:%M:%S') ==="
