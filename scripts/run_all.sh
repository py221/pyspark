#!/usr/bin/env bash
# Generate data, then run every stage script in order. Needs Java 17+ on PATH
# (or JAVA_HOME set) and the Python packages from requirements.txt.
set -euo pipefail
cd "$(dirname "$0")/.."
PY="${PYTHON:-python}"
"$PY" scripts/make_data.py > /dev/null
for f in 0[1-6]-*/[0-9]*.py; do
  echo "=== $f"
  "$PY" "$f" > /dev/null 2>&1 || { echo "FAILED: $f"; exit 1; }
done
echo "all stage scripts finished"
