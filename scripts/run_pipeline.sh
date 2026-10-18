#!/usr/bin/env bash
# Entry point for cron (Linux / Raspberry Pi) and Windows Task Scheduler (via wsl.exe).
#   scripts/run_pipeline.sh morning|afternoon [extra `main.py run` arguments]
# Output is appended to logs/cron.log; the pipeline's own log is logs/pipeline.log.
set -euo pipefail

SLOT="${1:?usage: run_pipeline.sh morning|afternoon [extra main.py run args]}"
shift

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

# cron starts with an almost empty environment
export PATH="$HOME/.local/bin:$HOME/.cargo/bin:/usr/local/bin:/usr/bin:/bin"
export TZ="Europe/Prague"
export LANG="${LANG:-C.UTF-8}"

# optional overrides and secrets, e.g. CLAUDE_CODE_OAUTH_TOKEN from `claude setup-token`
if [ -f .env ]; then
  set -a
  . ./.env
  set +a
fi
# with an API key in the environment the claude CLI would bill the API instead of the subscription
unset ANTHROPIC_API_KEY

mkdir -p logs
status=0
{
  echo "=== $(date '+%F %T %Z') run_pipeline.sh $SLOT $*"
  uv run --frozen main.py run --slot "$SLOT" "$@" || status=$?
  echo "=== exit $status"
} >> logs/cron.log 2>&1
exit "$status"
