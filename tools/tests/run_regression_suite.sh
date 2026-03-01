#!/bin/zsh
set -euo pipefail

cd "/Users/oscaralvarado/Documents/New project"

export AUTH_ENABLED=false
export ASYNC_EMBEDDINGS=true
export FAU_CORE_USE_FALLBACK_EMBEDDING=true

/Users/oscaralvarado/venv/bin/python tools/tests/generate_routes_snapshot.py
/Users/oscaralvarado/venv/bin/python tools/tests/check_deprecations.py
/Users/oscaralvarado/venv/bin/python tools/tests/evaluate_semantic_retrieval.py \
  --assert-thresholds \
  --hit-threshold 0.60 \
  --mrr-threshold 0.35 \
  --term-threshold 0.45 > /tmp/semantic_eval.json

# Suite de regresión sin plugin de cobertura para priorizar estabilidad de contratos HTTP.
/Users/oscaralvarado/venv/bin/python -m pytest -p no:cov -o addopts='-q' \
  test_route_snapshot.py \
  test_smoke_regression.py \
  test_api_regression_matrix.py \
  test_hospital_urgencias_regression.py \
  test_fau_bot_core_semantic_quality.py

echo "OK: regression suite completada"
