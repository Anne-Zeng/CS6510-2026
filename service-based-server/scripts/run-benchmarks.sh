#!/usr/bin/env bash
# 功能：运行教授要求的两组负载测试，并保存可提交的原始 JSON 与审计结果。
set -euo pipefail
cd "$(dirname "$0")/.."
# 功能：macOS 上仅在测试进程存活期间阻止空闲休眠，避免机器暂停使计时和连接失效。
# 其他平台没有 caffeinate 时直接执行；退出后防休眠状态由系统自动释放。
if command -v caffeinate >/dev/null 2>&1; then
    exec caffeinate -i python3 scripts/system.py benchmark "$@"
fi
exec python3 scripts/system.py benchmark "$@"
