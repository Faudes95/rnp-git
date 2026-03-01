# fau_BOT Core (Microservicio)

Microservicio IA central desacoplado del monolito, diseñado para:
- Lectura de datos clínicos/quirúrgicos con rol read-only.
- Escritura de salidas IA en esquema propio (`fau_bot_out`).
- Retrieval vectorial SQL real (`pgvector`) para conocimiento clínico.
- Integración de LLM local (Ollama/vLLM) con guardrails.
- Flujo Human-in-the-Loop (HITL) con auditoría.

## Variables de entorno
- `FAU_CORE_CLINICAL_RO_DSN` (read-only de `urologia`)
- `FAU_CORE_SURGICAL_RO_DSN` (read-only de `urologia_quirurgico`)
- `FAU_CORE_OUTPUT_DSN` (DB de salida; puede ser `urologia`)
- `FAU_CORE_OUTPUT_SCHEMA` (default `fau_bot_out`)
- `FAU_CORE_LLM_PROVIDER` (`none` | `ollama` | `vllm`)
- `FAU_CORE_LLM_BASE_URL`
- `FAU_CORE_LLM_MODEL`
- `FAU_CORE_LLM_API_KEY` (si aplica)
- `FAU_CORE_KNOWLEDGE_CHUNK_SIZE` (default `900`)
- `FAU_CORE_KNOWLEDGE_CHUNK_OVERLAP` (default `120`)
- `FAU_CORE_KNOWLEDGE_CANDIDATE_FACTOR` (default `8`)
- `FAU_CORE_KNOWLEDGE_MAX_CANDIDATES` (default `300`)
- `FAU_CORE_RUNTIME_PROFILE` (`dev` | `staging` | `prod`, default `prod`)
- `FAU_CORE_KPI_DEV_P95_MAX`, `FAU_CORE_KPI_DEV_ERROR_RATE_MAX`, `FAU_CORE_KPI_DEV_RESPONSE_MAX`
- `FAU_CORE_KPI_STAGING_P95_MAX`, `FAU_CORE_KPI_STAGING_ERROR_RATE_MAX`, `FAU_CORE_KPI_STAGING_RESPONSE_MAX`
- `FAU_CORE_KPI_PROD_P95_MAX`, `FAU_CORE_KPI_PROD_ERROR_RATE_MAX`, `FAU_CORE_KPI_PROD_RESPONSE_MAX`
- `FAU_CORE_KPI_<PROFILE>_REGRESSIONS_TARGET` (default `0`)

## Arranque
```bash
/Users/oscaralvarado/venv/bin/uvicorn fau_bot_core.main:app --host 0.0.0.0 --port 8010
```

## Endpoints principales
- `GET /status`
- `POST /run`
- `GET /alerts`
- `POST /knowledge/load-default`
- `GET /knowledge/search?q=...`
- `POST /dev/scan` (escaneo técnico para sugerencias de mejora aditivas)
- `GET /dev/suggestions` (cola de sugerencias `CODE_IMPROVEMENT`)
- `GET /architect/rules` (catálogo de reglas del Agente Arquitecto)
- `POST /architect/scan` (escaneo priorizado P0/P1/P2)
- `GET /architect/suggestions` (cola HITL `ARCHITECT_REVIEW`)
- `GET /hitl/suggestions`
- `POST /hitl/suggestions/{id}/status`
- `GET /hitl/audit`
- `GET /panel`

## Nota de despliegue
Los scripts SQL para crear rol read-only están en:
- `fau_bot_core/sql/setup_readonly_role_urologia.sql`
- `fau_bot_core/sql/setup_readonly_role_urologia_quirurgico.sql`

## Mejoras de retrieval (fase 3)
- Indexado por chunks de contenido (sin cambiar rutas).
- Ranking híbrido: vector + lexical + recencia.
- Expansión de consulta con sinónimos clínicos (litiasis, JJ, sepsis, sangrado, etc.).
- Consolidación de chunks en resultados finales para no duplicar documentos en UI.
