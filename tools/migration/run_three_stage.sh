#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
PYTHON_BIN="${PYTHON_BIN:-/Users/oscaralvarado/venv/bin/python}"
TOOL="${ROOT_DIR}/tools/migration/three_stage_migration.py"
STAMP="$(date +%Y%m%d_%H%M%S)"

SQLITE_CLINICAL_URL="${SQLITE_CLINICAL_URL:-sqlite:///${ROOT_DIR}/urologia.db}"
SQLITE_SURGICAL_URL="${SQLITE_SURGICAL_URL:-sqlite:///${ROOT_DIR}/urologia_quirurgico.db}"
PG_CLINICAL_URL="${PG_CLINICAL_URL:-postgresql+psycopg2://Faudes:1234@localhost:5432/urologia}"
PG_SURGICAL_URL="${PG_SURGICAL_URL:-postgresql+psycopg2://Faudes:1234@localhost:5432/urologia_quirurgico}"

RUNTIME_ENV="${ROOT_DIR}/backups/migration_runtime.env"
ROLLBACK_ENV="${ROOT_DIR}/backups/migration_runtime.rollback.env"
REPORT_DIR="${ROOT_DIR}/backups/migration_reports"
mkdir -p "${REPORT_DIR}" "${ROOT_DIR}/backups"

stage="${1:-}"
if [[ -z "${stage}" ]]; then
  echo "Uso: $0 shadow|dual-write|cutover|finalize|rollback|validate"
  exit 2
fi

common_args=(
  --sqlite-clinical-url "${SQLITE_CLINICAL_URL}"
  --sqlite-surgical-url "${SQLITE_SURGICAL_URL}"
  --pg-clinical-url "${PG_CLINICAL_URL}"
  --pg-surgical-url "${PG_SURGICAL_URL}"
)

run_tool() {
  "${PYTHON_BIN}" "${TOOL}" "${common_args[@]}" "$@"
}

case "${stage}" in
  shadow)
    run_tool \
      --report "${REPORT_DIR}/shadow_${STAMP}.json" \
      --env-out "${RUNTIME_ENV}" \
      shadow
    ;;
  dual-write)
    run_tool \
      --report "${REPORT_DIR}/dual_write_${STAMP}.json" \
      --env-out "${RUNTIME_ENV}" \
      emit-env --stage dual_write
    ;;
  cutover)
    run_tool \
      --report "${REPORT_DIR}/pre_cutover_validate_${STAMP}.json" \
      validate
    run_tool \
      --report "${REPORT_DIR}/cutover_${STAMP}.json" \
      --env-out "${RUNTIME_ENV}" \
      emit-env --stage cutover
    run_tool \
      --report "${REPORT_DIR}/rollback_${STAMP}.json" \
      --env-out "${ROLLBACK_ENV}" \
      emit-env --stage rollback
    ;;
  finalize)
    run_tool \
      --report "${REPORT_DIR}/finalize_${STAMP}.json" \
      --env-out "${RUNTIME_ENV}" \
      emit-env --stage finalize
    ;;
  rollback)
    run_tool \
      --report "${REPORT_DIR}/rollback_${STAMP}.json" \
      --env-out "${RUNTIME_ENV}" \
      emit-env --stage rollback
    ;;
  validate)
    run_tool \
      --report "${REPORT_DIR}/validate_${STAMP}.json" \
      validate
    ;;
  *)
    echo "Etapa no soportada: ${stage}"
    exit 2
    ;;
esac

echo "OK - etapa ${stage} completada"
echo "Env runtime: ${RUNTIME_ENV}"
if [[ -f "${ROLLBACK_ENV}" ]]; then
  echo "Env rollback: ${ROLLBACK_ENV}"
fi
echo "Reportes: ${REPORT_DIR}"
echo "Para aplicar variables al shell actual:"
echo "  set -a; source \"${RUNTIME_ENV}\"; set +a"
