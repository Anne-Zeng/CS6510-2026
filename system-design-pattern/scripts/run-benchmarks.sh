#!/usr/bin/env bash
# 功能：使用临时数据库运行原始 load-client 并进行跨库审计；失败保留报告并返回非零。
set -euo pipefail
cd "$(dirname "$0")/.."
if command -v caffeinate >/dev/null 2>&1; then
    exec caffeinate -i python3 scripts/benchmark-isolated.py "$@"
fi
exec python3 scripts/benchmark-isolated.py "$@"
