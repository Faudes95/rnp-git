#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/Users/oscaralvarado/Documents/New project"
MODE_FILE="${PROJECT_DIR}/backups/runtime_mode.env"
MIGRATION_ENV_FILE="${PROJECT_DIR}/backups/migration_runtime.env"
MODE="${1:-}"

if [[ -z "${MODE}" ]]; then
  echo "Uso: $0 sqlite|postgres"
  exit 1
fi

MODE="$(echo "${MODE}" | tr '[:upper:]' '[:lower:]')"
if [[ "${MODE}" != "sqlite" && "${MODE}" != "postgres" ]]; then
  echo "Modo invalido: ${MODE}. Usa sqlite o postgres."
  exit 1
fi

if [[ "${MODE}" == "postgres" ]]; then
  if [[ ! -f "${MIGRATION_ENV_FILE}" ]]; then
    echo "Falta ${MIGRATION_ENV_FILE}. No se puede activar postgres."
    exit 1
  fi
  if ! grep -q '^DATABASE_URL=' "${MIGRATION_ENV_FILE}" || ! grep -q '^SURGICAL_DATABASE_URL=' "${MIGRATION_ENV_FILE}"; then
    echo "migration_runtime.env no tiene DATABASE_URL y SURGICAL_DATABASE_URL."
    exit 1
  fi
fi

cat > "${MODE_FILE}" <<EOF
RNP_RUNTIME_MODE=${MODE}
EOF

echo "Modo persistente configurado: ${MODE}"
echo "Archivo: ${MODE_FILE}"
