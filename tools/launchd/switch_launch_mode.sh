#!/usr/bin/env bash
set -euo pipefail

MODE="${1:-}"
AGENTS_DIR="/Users/oscaralvarado/Library/LaunchAgents"

SQLITE_UV="${AGENTS_DIR}/com.rnp.uvicorn.service.plist"
SQLITE_CE="${AGENTS_DIR}/com.rnp.celery.service.plist"
PG_UV="${AGENTS_DIR}/com.rnp.uvicorn.cutover.plist"
PG_CE="${AGENTS_DIR}/com.rnp.celery.cutover.plist"

if [[ -z "${MODE}" ]]; then
  echo "Uso: $0 sqlite|postgres"
  exit 1
fi

MODE="$(echo "${MODE}" | tr '[:upper:]' '[:lower:]')"
if [[ "${MODE}" != "sqlite" && "${MODE}" != "postgres" ]]; then
  echo "Modo invalido: ${MODE}. Usa sqlite o postgres."
  exit 1
fi

if [[ "${MODE}" == "sqlite" ]]; then
  launchctl bootout gui/501 "${PG_UV}" >/dev/null 2>&1 || true
  launchctl bootout gui/501 "${PG_CE}" >/dev/null 2>&1 || true
  launchctl bootout gui/501 "${SQLITE_UV}" >/dev/null 2>&1 || true
  launchctl bootout gui/501 "${SQLITE_CE}" >/dev/null 2>&1 || true
  launchctl bootstrap gui/501 "${SQLITE_UV}"
  launchctl bootstrap gui/501 "${SQLITE_CE}"
  launchctl kickstart -k gui/501/com.rnp.uvicorn.service
  launchctl kickstart -k gui/501/com.rnp.celery.service
  echo "Modo activo: SQLITE"
else
  if [[ ! -f "${PG_UV}" || ! -f "${PG_CE}" ]]; then
    echo "Faltan plists de postgres (cutover)."
    echo "Revisar: ${PG_UV} y ${PG_CE}"
    exit 1
  fi
  launchctl bootout gui/501 "${SQLITE_UV}" >/dev/null 2>&1 || true
  launchctl bootout gui/501 "${SQLITE_CE}" >/dev/null 2>&1 || true
  launchctl bootout gui/501 "${PG_UV}" >/dev/null 2>&1 || true
  launchctl bootout gui/501 "${PG_CE}" >/dev/null 2>&1 || true
  launchctl bootstrap gui/501 "${PG_UV}"
  launchctl bootstrap gui/501 "${PG_CE}"
  launchctl kickstart -k gui/501/com.rnp.uvicorn.cutover
  launchctl kickstart -k gui/501/com.rnp.celery.cutover
  echo "Modo activo: POSTGRES (cutover)"
fi

launchctl list | grep -E "com.rnp.(uvicorn|celery).(service|cutover)" || true
