#!/usr/bin/env bash
# Build the Directwire agent-fast-transport Internet-Draft: .xml -> .txt / .html via xml2rfc.
# Pure-python toolchain (no ruby needed); works locally and in CI.
# Optional: runs `idnits` (perl) as a submission-preflight check if installed.
set -euo pipefail
cd "$(dirname "$0")"

XML="draft-directwire-agent-fast-transport-00.xml"
BASE="${XML%.xml}"

# --- 1) find a working python (skip the Windows Store "python3" stub) --------
pick_python() {
  for cand in python3 python py; do
    if command -v "$cand" >/dev/null 2>&1 && "$cand" -c 'import sys' >/dev/null 2>&1; then
      echo "$cand"
      return
    fi
  done
  echo "python3"   # last resort; CI runners have a real python3
}

# --- 2) ensure xml2rfc exists in a local venv --------------------------------
X2R=""
if [ -x ".venv/bin/xml2rfc" ]; then
  X2R=".venv/bin/xml2rfc"
elif [ -x ".venv/Scripts/xml2rfc.exe" ]; then
  X2R=".venv/Scripts/xml2rfc.exe"
fi
if [ -z "$X2R" ]; then
  PY="$(pick_python)"
  echo ">> creating .venv with $PY"
  "$PY" -m venv .venv
  # Pin the exact xml2rfc version that produced the committed artifacts:
  # a newer release could change rendering and trip the "artifacts are
  # current" check in CI. Bump the version + regenerate + commit together.
  if [ -x ".venv/bin/pip" ]; then
    .venv/bin/pip install --quiet xml2rfc==3.34.0
    X2R=".venv/bin/xml2rfc"
  else
    .venv/Scripts/pip.exe install --quiet xml2rfc==3.34.0
    X2R=".venv/Scripts/xml2rfc.exe"
  fi
fi

# --- 3) build ----------------------------------------------------------------
"$X2R" --text --html "$XML"
echo "OK: $XML -> $BASE.txt $BASE.html"

# --- 4) idnits preflight (advisory; requires perl) ---------------------------
if command -v idnits >/dev/null 2>&1; then
  echo ">> idnits:"
  idnits "$BASE.txt" && echo "idnits: no issues"
else
  echo "(idnits not installed; run it before submission:"
  echo "  curl -fsSL -o idnits https://raw.githubusercontent.com/ietf-tools/idnits/main/idnits && bash idnits $BASE.txt)"
fi
