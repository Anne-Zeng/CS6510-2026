#!/usr/bin/env bash
# Builds and runs the packaged jar.
#
# Deliberately NOT `mvnw spring-boot:run`: that launcher passes
# -XX:TieredStopAtLevel=1, which caps the JVM at the C1 compiler and leaves the
# optimizing C2 compiler switched off. It makes restarts feel faster during
# development and makes every throughput measurement several times too slow.
set -euo pipefail
cd "$(dirname "$0")"

jar="target/layered-server-0.0.1-SNAPSHOT.jar"
if [[ "${SKIP_BUILD:-0}" != "1" || ! -f "$jar" ]]; then
  ./mvnw -q clean package -DskipTests
fi
exec java -jar "$jar" "$@"
