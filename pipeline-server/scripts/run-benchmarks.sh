#!/usr/bin/env bash
# Runs the default and stress workloads against the pipeline server and writes the
# two JSON reports and independent correctness audits.
#
# Each mode resets only a dedicated pipeline_acceptance* database.
set -euo pipefail
cd "$(dirname "$0")/.."
project="$PWD"

port="${BENCHMARK_PORT:-18085}"
db_host="${CHECKOUT_DB_HOST:-localhost}"
db_port="${CHECKOUT_DB_PORT:-5432}"
db_name="${CHECKOUT_DB_NAME:-pipeline_acceptance}"
db_user="${CHECKOUT_DB_USER:-postgres}"
db_password="${CHECKOUT_DB_PASSWORD:-postgres}"
catalog_size="${CHECKOUT_CATALOG_SIZE:-2000}"
# Opening stock must outlast the workload or the benchmark measures the fixture
# rather than the server: the Zipf sampler sends ~12% of all scans to
# SKU-000001, which exhausted the spec's 10,000 units inside 60s. See README.
stock_per_item="${CHECKOUT_STOCK_PER_ITEM:-1000000}"

export PGPASSWORD="$db_password"
psql_admin=(psql -h "$db_host" -p "$db_port" -U "$db_user" -d postgres -v ON_ERROR_STOP=1 -qtA)
psql_app=(psql -h "$db_host" -p "$db_port" -U "$db_user" -d "$db_name" -v ON_ERROR_STOP=1 -qtA)

for tool in lsof curl python3 psql javac java; do
  command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 1; }
done

if [[ ! "$db_name" =~ ^pipeline_acceptance[a-zA-Z0-9_]*$ ]]; then
  echo "Use a dedicated database whose name starts with pipeline_acceptance." >&2
  exit 1
fi

if lsof -nP -iTCP:"$port" -sTCP:LISTEN >/dev/null 2>&1; then
  echo "Port $port is occupied. Choose a free BENCHMARK_PORT." >&2
  exit 1
fi

# A second JVM or server competing for CPU invalidates the measurement.
if lsof -nP -iTCP:8080 -sTCP:LISTEN >/dev/null 2>&1; then
  echo "WARNING: something is listening on :8080 (the monolith?). Stop it for a clean run." >&2
fi

"${psql_admin[@]}" -c "SELECT 1 FROM pg_database WHERE datname = '$db_name';" | grep -q 1 \
  || "${psql_admin[@]}" -c "CREATE DATABASE $db_name;"

if [[ "${SKIP_BUILD:-0}" != "1" ]]; then
  ./mvnw -q clean package
fi
jar="target/pipeline-server-0.0.1-SNAPSHOT.jar"
[[ -f "$jar" ]] || { echo "Missing $jar" >&2; exit 1; }

mkdir -p reports target/load-client
# Compile the professor's unmodified load client into this project's build dir.
javac --release 21 -d target/load-client ../load-client/src/*.java

run_dir="$project/reports/run-$(date +%Y%m%d-%H%M%S)"
mkdir -p "$run_dir"

server_pid=""
stop_server() {
  if [[ -n "$server_pid" ]]; then
    kill -TERM "$server_pid" 2>/dev/null || true
    wait "$server_pid" 2>/dev/null || true
    server_pid=""
  fi
}
trap stop_server EXIT INT TERM

start_server() {
  local log="$1"
  java -jar "$jar" \
    --server.port="$port" \
    --spring.datasource.url="jdbc:postgresql://$db_host:$db_port/$db_name" \
    --spring.datasource.username="$db_user" \
    --spring.datasource.password="$db_password" \
    --checkout.catalog-size="$catalog_size" \
    --checkout.stock-per-item="$stock_per_item" \
    > "$log" 2>&1 &
  server_pid=$!
  for _ in {1..180}; do
    if ! kill -0 "$server_pid" 2>/dev/null; then
      echo "Server exited early. See $log" >&2
      tail -30 "$log" >&2
      return 1
    fi
    # The port opens before seeding finishes, so require the whole catalog.
    if curl -fsS "http://localhost:$port/items" 2>/dev/null | python3 -c \
        'import json,sys; sys.exit(0 if len(json.load(sys.stdin)["items"]) == int(sys.argv[1]) else 1)' \
        "$catalog_size" 2>/dev/null; then
      return 0
    fi
    sleep 1
  done
  echo "Server did not become ready. See $log" >&2
  return 1
}

for mode in default stress; do
  if [[ "$mode" == "default" ]]; then stations=10; duration=60; else stations=100; duration=120; fi
  output="$run_dir/$mode"
  mkdir -p "$output"
  echo
  echo "=== $mode: $stations stations for ${duration}s ==="

  # Fresh state in the dedicated acceptance database only.
  "${psql_app[@]}" -c "DROP SCHEMA public CASCADE; CREATE SCHEMA public;" >/dev/null

  start_server "$output/server.log"

  java -cp target/load-client Main \
    --baseUrl="http://localhost:$port" \
    --stations="$stations" --duration="$duration" \
    --reportDir="$output" | tee "$output/client.log"

  report_files=("$output"/report-*.json)
  [[ "${#report_files[@]}" == 1 && -f "${report_files[0]}" ]] \
    || { echo "Expected exactly one report in $output" >&2; exit 1; }

  # Drain every due window before comparing persistence across a restart.
  expected_end=$("${psql_app[@]}" -c "SELECT COALESCE(MAX(sequence),0)/500*500 FROM scan_events;")
  drained=0
  for _ in {1..120}; do
    actual_end=$(curl -fsS "http://localhost:$port/analytics/popular-items" | python3 -c 'import json,sys; print(json.load(sys.stdin)["windowEnd"])')
    if [[ "$actual_end" == "$expected_end" ]]; then drained=1; break; fi
    sleep 1
  done
  [[ "$drained" == 1 ]] || { echo "Analytics did not drain" >&2; exit 1; }

  # Save the ranking the API serves, then prove it survives a restart.
  curl -fsS "http://localhost:$port/analytics/popular-items" > "$output/popular-items.json"
  stop_server
  start_server "$output/restart.log"
  curl -fsS "http://localhost:$port/analytics/popular-items" > "$output/popular-items-after-restart.json"
  diff <(python3 -m json.tool "$output/popular-items.json") \
       <(python3 -m json.tool "$output/popular-items-after-restart.json") \
    || { echo "Analytics ranking did not survive a restart" >&2; exit 1; }
  stop_server

  # Seed parameters are passed explicitly so the audit cannot silently drift
  # out of step with checkout.catalog-size / checkout.stock-per-item.
  python3 scripts/verify_run.py --report "${report_files[0]}" \
    --host "$db_host" --port "$db_port" --dbname "$db_name" \
    --user "$db_user" --password "$db_password" \
    --catalog-size "$catalog_size" --initial-stock "$stock_per_item" \
    | tee "$output/correctness.json"

  cp "${report_files[0]}" "$run_dir/report-$mode.json"
  echo "$mode: report written, correctness audit and restart check passed."
done

echo
echo "Submission reports:"
echo "  $run_dir/report-default.json"
echo "  $run_dir/report-stress.json"
