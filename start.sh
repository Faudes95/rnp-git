#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

HOST="127.0.0.1"
PORT="8000"
QUICK="false"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --quick)
      QUICK="true"
      shift
      ;;
    --host)
      HOST="${2:-127.0.0.1}"
      shift 2
      ;;
    --port)
      PORT="${2:-8000}"
      shift 2
      ;;
    *)
      echo "Argumento no reconocido: $1" >&2
      exit 1
      ;;
  esac
done

PYTHON_BIN="python3"
if [[ -x "$SCRIPT_DIR/.venv/bin/python" ]]; then
  PYTHON_BIN="$SCRIPT_DIR/.venv/bin/python"
fi

PID_FILES=(
  "$SCRIPT_DIR/.uvicorn_codex.pid"
  "$SCRIPT_DIR/.uvicorn_local.pid"
)

cleanup_previous_uvicorn() {
  for pid_file in "${PID_FILES[@]}"; do
    if [[ -f "$pid_file" ]]; then
      pid="$(cat "$pid_file" 2>/dev/null || true)"
      if [[ -n "${pid:-}" ]] && kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
      fi
      rm -f "$pid_file"
    fi
  done

  pgrep -f "uvicorn main:app" | while read -r pid; do
    kill "$pid" 2>/dev/null || true
  done || true

  lsof -ti "tcp:${PORT}" | while read -r pid; do
    kill "$pid" 2>/dev/null || true
  done || true

  sleep 1
}

echo ""
echo "╔══════════════════════════════════════════════════════════╗"
echo "║                UROMED · arranque local                  ║"
echo "╚══════════════════════════════════════════════════════════╝"
echo ""

echo "Limpiando instancias previas de uvicorn..."
cleanup_previous_uvicorn

export IMSS_USER="${IMSS_USER:-Faudes}"
export IMSS_PASS="${IMSS_PASS:-1995}"
export AUTH_ENABLED="${AUTH_ENABLED:-true}"
export ALLOW_INSECURE_DEFAULT_CREDENTIALS="${ALLOW_INSECURE_DEFAULT_CREDENTIALS:-true}"
export STARTUP_INTERCONEXION_MODE="${STARTUP_INTERCONEXION_MODE:-off}"
export AI_WARMUP_MODE="${AI_WARMUP_MODE:-off}"

if [[ "$QUICK" != "true" ]]; then
  echo "Precompilando app/ y fau_bot_core/..."
  "$PYTHON_BIN" -m compileall -q app fau_bot_core main.py || true
fi

echo "Iniciando servidor..."
echo "URL:      http://${HOST}:${PORT}"
echo "Health:   http://${HOST}:${PORT}/status"
echo "Usuario:  ${IMSS_USER}"
echo "Clave:    ${IMSS_PASS}"
echo "Startup:  interconexion=${STARTUP_INTERCONEXION_MODE} | ai=${AI_WARMUP_MODE}"
echo ""

echo $$ > "$SCRIPT_DIR/.uvicorn_local.pid"
exec "$PYTHON_BIN" -m uvicorn main:app --host "$HOST" --port "$PORT"
