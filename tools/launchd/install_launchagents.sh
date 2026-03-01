#!/usr/bin/env bash
set -euo pipefail

PROJECT_DIR="/Users/oscaralvarado/Documents/New project"
AGENTS_DIR="/Users/oscaralvarado/Library/LaunchAgents"

mkdir -p "${AGENTS_DIR}"

cp "${PROJECT_DIR}/tools/launchd/com.rnp.uvicorn.service.plist" "${AGENTS_DIR}/com.rnp.uvicorn.service.plist"
cp "${PROJECT_DIR}/tools/launchd/com.rnp.celery.service.plist" "${AGENTS_DIR}/com.rnp.celery.service.plist"

chmod +x "${PROJECT_DIR}/tools/launchd/run_uvicorn.sh"
chmod +x "${PROJECT_DIR}/tools/launchd/run_celery.sh"
chmod +x "${PROJECT_DIR}/tools/launchd/set_mode.sh"

echo "LaunchAgents instalados:"
echo "- ${AGENTS_DIR}/com.rnp.uvicorn.service.plist"
echo "- ${AGENTS_DIR}/com.rnp.celery.service.plist"
echo
echo "Siguiente paso (manual):"
echo "launchctl bootout gui/501 ${AGENTS_DIR}/com.rnp.uvicorn.service.plist >/dev/null 2>&1 || true"
echo "launchctl bootout gui/501 ${AGENTS_DIR}/com.rnp.celery.service.plist >/dev/null 2>&1 || true"
echo "launchctl bootstrap gui/501 ${AGENTS_DIR}/com.rnp.uvicorn.service.plist"
echo "launchctl bootstrap gui/501 ${AGENTS_DIR}/com.rnp.celery.service.plist"
echo "launchctl kickstart -k gui/501/com.rnp.uvicorn.service"
echo "launchctl kickstart -k gui/501/com.rnp.celery.service"
