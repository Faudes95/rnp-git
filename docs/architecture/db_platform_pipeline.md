# Pipeline de plataforma de datos PostgreSQL

## Objetivo
Formalizar la revisión y aplicación de los assets SQL que sostienen analytics, particionado y búsqueda vectorial, sin mezclar esa lógica con el composition root clínico.

## Assets registrados
- `materialized_views`
  - archivo: `materialized_views.sql`
  - alcance: `clinical`
  - propósito: agregados/materialized views para analytics
- `partition_vitals`
  - archivo: `partition_vitals.sql`
  - alcance: `clinical`
  - propósito: particionado de `vitals`
  - política: `manual review required`
- `migration_vector`
  - archivo: `migration_vector.sql`
  - alcance: `clinical`
  - propósito: `pgvector` + índice vectorial para `clinical_notes`

## CLI
Archivo:
- `scripts/db_platform_pipeline.py`

### Review
```bash
python scripts/db_platform_pipeline.py review \
  --dsn postgresql://postgres:postgres@127.0.0.1:5432/uromed_clinical \
  --database-scope clinical \
  --json
```

### Apply seguro
```bash
python scripts/db_platform_pipeline.py apply \
  --dsn postgresql://postgres:postgres@127.0.0.1:5432/uromed_clinical \
  --database-scope clinical \
  --asset materialized_views \
  --asset migration_vector \
  --json
```

### Apply de asset manual
```bash
python scripts/db_platform_pipeline.py apply \
  --dsn postgresql://postgres:postgres@127.0.0.1:5432/uromed_clinical \
  --database-scope clinical \
  --asset partition_vitals \
  --allow-manual-review \
  --json
```

## Health checks expuestos
- `/admin/database/status`
- `/api/v1/database/status`

Nuevos campos relevantes:
- `platform_assets_ready`
- `clinical.platform_assets`
- `surgical.platform_assets`

## Política operativa
- `materialized_views` y `migration_vector` son candidatos a autoaplicación controlada.
- `partition_vitals` no se autoaplica en CI porque su conversión puede requerir recreación de tabla, ventana de mantenimiento o estrategia de backfill.
- La readiness a Postgres ya no depende solo de `SELECT 1` y `pgvector`; ahora también depende de assets SQL observables.

## CI
Workflow:
- `.github/workflows/db-platform.yml`

Ese workflow:
- levanta PostgreSQL con imagen `pgvector`
- crea un schema mínimo de prueba
- corre `review`
- aplica assets seguros
- vuelve a correr `review`
- sube artifacts JSON
