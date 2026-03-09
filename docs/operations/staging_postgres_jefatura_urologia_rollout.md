# Staging PostgreSQL para `APP_BOOT_PROFILE=jefatura_urologia`

## Objetivo
- Validar `Jefatura de Urología`, `Central` y `Perfiles de Residentes` sobre PostgreSQL real.
- Mantener la misma disciplina operativa usada para `quirofano`: review/apply de assets, smoke del perfil y E2E profunda del dominio.

## Variables de entorno
```bash
export APP_BOOT_PROFILE=jefatura_urologia
export DATABASE_PLATFORM_TARGET=postgres
export DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/uromed_clinical
export SURGICAL_DATABASE_URL=postgresql://postgres:postgres@127.0.0.1:5432/uromed_surgical
export STARTUP_INTERCONEXION_MODE=off
export AI_WARMUP_MODE=off
export AUTH_ENABLED=true
export AUTH_USER=Faudes
export AUTH_PASS=1995
export IMSS_USER=Faudes
export IMSS_PASS=1995
export BASE_URL=http://127.0.0.1:8000
```

## Orden recomendado
1. Crear o limpiar `uromed_clinical` y `uromed_surgical`.
2. Arrancar `full` una vez para revisar/aplicar assets SQL:
   - `python scripts/db_platform_pipeline.py review --dsn "$DATABASE_URL" --json`
   - `python scripts/db_platform_pipeline.py apply --dsn "$DATABASE_URL" --asset materialized_views --asset migration_vector --json`
   - `python scripts/db_platform_pipeline.py review --dsn "$DATABASE_URL" --json`
3. Verificar `GET /admin/database/status` y `GET /api/v1/database/status` con auth.
4. Reiniciar con `APP_BOOT_PROFILE=jefatura_urologia`.
5. Confirmar `/status` y que el manifest expone `jefaturas`, `shell`, `auth_login` y `api_v1`.
6. Ejecutar:
   - `npm run e2e:staging:jefatura_urologia`

## Criterios de listo
- `target_mode=postgres`
- `clinical.mode=postgres`
- `surgical.mode=postgres`
- `platform_assets_ready=true` salvo `partition_vitals`, que sigue en `manual_review_required`
- Suite verde de:
  - `jefatura_central.spec.ts`
  - `jefatura_urologia_residentes.spec.ts`
  - `jefatura_urologia_crossflow.spec.ts`

## Alcance del perfil
- Incluye:
  - `Jefatura de Urología`
  - `Central`
  - `Perfiles de residentes`
  - `auth_login`
  - `api_v1`
- Excluye:
  - `hospitalizacion`
  - `consulta`
  - `expediente`
  - `quirofano`
  - `jefatura_quirofano`

## Siguiente corte natural
- Si este rollout queda verde en PostgreSQL staging, el siguiente perfil interno a operar es `residentes_urologia` o bien un perfil combinado de `jefaturas` para pilotaje supervisado.
