#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/Users/oscaralvarado/Documents/New project"
VENV_BIN="/Users/oscaralvarado/venv/bin"
MODE_FILE="${PROJECT_DIR}/backups/runtime_mode.env"
MIGRATION_ENV_FILE="${PROJECT_DIR}/backups/migration_runtime.env"

cd "${PROJECT_DIR}"

export PATH="/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin"
export PYTHONUNBUFFERED=1
export AUTH_ENABLED="${AUTH_ENABLED:-false}"
export MPLCONFIGDIR="/tmp/matplotlib"
mkdir -p "${MPLCONFIGDIR}"

mode="sqlite"
if [[ -f "${MODE_FILE}" ]]; then
  # shellcheck disable=SC1090
  source "${MODE_FILE}"
  mode="${RNP_RUNTIME_MODE:-sqlite}"
fi
mode="$(echo "${mode}" | tr '[:upper:]' '[:lower:]')"

if [[ "${mode}" == "postgres" ]]; then
  if [[ -f "${MIGRATION_ENV_FILE}" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "${MIGRATION_ENV_FILE}"
    set +a
  fi
else
  unset DATABASE_URL SURGICAL_DATABASE_URL CLINICAL_SHADOW_DATABASE_URL SURGICAL_SHADOW_DATABASE_URL
  unset DB_MIGRATION_STAGE DB_DUAL_WRITE
fi

exec "${VENV_BIN}/uvicorn" main:app --host 0.0.0.0 --port 8000
