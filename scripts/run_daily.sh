#!/usr/bin/env bash
# =============================================================================
# run_daily.sh — Daily BD automation run
# =============================================================================
# Intended cron schedule (run at 7:45am daily, results ready by 8am):
#   45 7 * * 1-5 /home/johnserra/Projects/business-development/scripts/run_daily.sh >> /var/log/bd-daily.log 2>&1
#
# Or with APScheduler (see scripts/scheduler.py — future phase).
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

echo "=== BD Daily Run — $(date '+%Y-%m-%d %H:%M:%S') ==="

# --- Module 6: Follow-up Scheduler ---
# Creates Odoo activities and sends morning digest
echo "[1/2] Running Follow-up Scheduler..."
uv run python -m modules.followup_scheduler.main
echo "      Done."

# --- Module 4: Lead Scoring ---
# Re-scores all active leads, advances qualified ones
echo "[2/2] Running Lead Scoring..."
uv run python -m modules.lead_scoring.main
echo "      Done."

echo "=== Daily run complete — $(date '+%H:%M:%S') ==="
