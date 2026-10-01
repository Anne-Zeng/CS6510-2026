#!/usr/bin/env bash
# Builds and runs the packaged jar.
#
# Uses the same packaged-JAR launch method as the layered benchmark.
set -euo pipefail
cd "$(dirname "$0")"

jar="target/pipeline-server-0.0.1-SNAPSHOT.jar"
if [[ "${SKIP_BUILD:-0}" != "1" || ! -f "$jar" ]]; then
  ./mvnw -q clean package -DskipTests
fi
exec java -jar "$jar" "$@"
