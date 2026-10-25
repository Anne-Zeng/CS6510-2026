#!/usr/bin/env python3
"""运行四个临时数据库、真实微服务与 Gateway/run.sh 的隔离验收，不重置正常数据库。"""
from pathlib import Path
import os
import sys
script=Path(__file__).resolve().parents[1]/'checkout-service/scripts/test-saga-isolated.py'
os.execv(sys.executable,[sys.executable,str(script),*sys.argv[1:]])
