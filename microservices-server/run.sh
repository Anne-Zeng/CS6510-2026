#!/usr/bin/env bash
# 功能：从任意工作目录启动整个本地系统，Ctrl+C 自动关闭五个进程。
set -euo pipefail
cd "$(dirname "$0")"
exec python3 scripts/system.py serve "$@"
