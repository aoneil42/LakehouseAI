#!/usr/bin/env python3
import sys
import urllib.request

try:
    urllib.request.urlopen("http://localhost:8090/api/agent/health", timeout=5)
    sys.exit(0)
except Exception:
    sys.exit(1)
