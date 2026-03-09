# Staging PostgreSQL + Rollout interno del perfil `quirofano`

## Objetivo
Operar UROMED sobre PostgreSQL real en staging y validar el primer perfil interno clínico (`APP_BOOT_PROFILE=quirofano`) sin tocar rutas ni payloads.

## Variables obligatorias
```bash
export APP_BOOT_PROFILE=full
export DATABASE_PLATFORM_TARGET=postgres
export DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/uromed_clinical
export SURGICAL_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/uromed_surgical
export STARTUP_INTERCONEXION_MODE=off
export AI_WARMUP_MODE=off
export IMSS_USER=Faudes
export IMSS_PASS=1995
export AUTH_USER=Faudes
export AUTH_PASS=1995
```

## Orden de arranque
1. Crear las bases `uromed_clinical` y `uromed_surgical`.
2. Levantar la app con `APP_BOOT_PROFILE=full`.
3. Esperar `/status`.
4. Ejecutar:
```bash
python scripts/db_platform_pipeline.py review --dsn "$DATABASE_URL" --json
python scripts/db_platform_pipeline.py apply --dsn "$DATABASE_URL" --asset materialized_views --asset migration_vector --json
python scripts/db_platform_pipeline.py review --dsn "$DATABASE_URL" --json
```
5. Verificar:
   - `/admin/database/status`
   - `/api/v1/database/status`
6. Ejecutar la suite `full`.
7. Reiniciar con `APP_BOOT_PROFILE=quirofano`.
8. Ejecutar la suite acotada de quirófano.

## Criterio de listo
- `target_mode=postgres`
- `ready_for_target=true` o, en primera pasada, motores PostgreSQL + `pgvector` correctos
- `platform_assets_ready=true`, dejando `partition_vitals` en manual review
- `full` verde o con hallazgos no bloqueantes documentados
- `quirofano` verde en:
  - urgencias
  - programada
  - lista de espera
  - jefatura de quirófano
  - importación PDF/conciliación

## Notas
- `partition_vitals.sql` no se autoaplica en este corte.
- `main_full.py` sigue siendo composition root temporal.
- El siguiente rollout interno, si este corte sale verde, es `jefatura_urologia`.
