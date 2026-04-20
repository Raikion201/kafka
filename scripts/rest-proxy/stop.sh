#!/usr/bin/env bash
# Force-stop a running Kafka broker started by start.sh.
# On Windows the regular Ctrl-C in the start.sh terminal is the preferred
# shutdown path; use this script when you've lost the terminal or when a
# crash left a JVM holding open log files.
#
# Usage:
#   bash scripts/rest-proxy/stop.sh
set -euo pipefail

echo "[stop.sh] Killing any java.exe running kafka.Kafka ..."
powershell "Get-CimInstance Win32_Process -Filter \"Name = 'java.exe'\" | \
  Where-Object { \$_.CommandLine -match 'kafka.Kafka' } | \
  ForEach-Object { Write-Host ('    killed pid ' + \$_.ProcessId); Stop-Process -Id \$_.ProcessId -Force }" \
  2>&1 || true

echo "[stop.sh] Done."
