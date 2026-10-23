#!/usr/bin/env bash
# Copy code changes to the Raspberry Pi and restart the watcher.
#   scripts/deploy_pi.sh [user@host]
# Never touches the Pi's data/ (the watcher's state), logs/ (the watchdog cron's output) or .env.
set -euo pipefail

HOST="${1:-boobogo@192.168.0.55}"
DEST="investment"
cd "$(dirname "${BASH_SOURCE[0]}")/.."

rsync -az --delete \
  --exclude .venv --exclude .git --exclude __pycache__ --exclude .pytest_cache \
  --exclude data --exclude logs --exclude .env \
  ./ "$HOST:$DEST/"

ssh "$HOST" "cd $DEST && ~/.local/bin/uv sync --frozen --quiet && sudo systemctl restart market-watcher \
  && sleep 5 && systemctl --no-pager --lines=0 status market-watcher | head -3"
