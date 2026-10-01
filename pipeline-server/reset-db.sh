#!/usr/bin/env bash
# Explicit destructive reset of the selected pipeline database; not used by acceptance tests.
set -euo pipefail
cd "$(dirname "$0")"

db_host="${CHECKOUT_DB_HOST:-localhost}"
db_port="${CHECKOUT_DB_PORT:-5432}"
db_name="${CHECKOUT_DB_NAME:-pipelinedb}"
db_user="${CHECKOUT_DB_USER:-postgres}"
export PGPASSWORD="${CHECKOUT_DB_PASSWORD:-postgres}"

if [[ ! "$db_name" =~ ^pipeline[a-zA-Z0-9_]*$ ]]; then
  echo "Only pipeline-prefixed database names may be reset." >&2
  exit 1
fi

if lsof -nP -iTCP:"${PORT:-8080}" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Stop the pipeline server before resetting its database." >&2
  exit 1
fi

psql -h "$db_host" -p "$db_port" -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 \
  -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;"

echo "Reset $db_name. Start the server to reseed 2,000 items with the configured opening stock (default 1,000,000 units each)."
