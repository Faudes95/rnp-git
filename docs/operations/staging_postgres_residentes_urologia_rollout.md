# Staging PostgreSQL para `APP_BOOT_PROFILE=residentes_urologia`

## Objetivo
- Validar el perfil interno longitudinal de residentes sobre PostgreSQL real.
- Comprobar que el corredor `postquirúrgica -> indexación -> perfil del residente` y la interconectividad con `Central` se mantienen operativas sin cargar dominios ajenos.

## Variables de entorno
```bash
export APP_BOOT_PROFILE=residentes_urologia
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
1. Asegurar que los assets SQL ya fueron revisados/aplicados en `full`.
2. Verificar `GET /api/v1/database/status` con auth.
3. Reiniciar con `APP_BOOT_PROFILE=residentes_urologia`.
4. Confirmar `/status`.
5. Verificar que el manifest expone `jefaturas`, `shell`, `auth_login` y `api_v1`.
6. Ejecutar:
   - `npm run e2e:staging:residentes_urologia`

## Criterios de listo
- Motores clínico y quirúrgico en PostgreSQL.
- `platform_assets_ready=true` salvo `partition_vitals` manual.
- Suite verde de:
  - `jefatura_urologia_residentes.spec.ts`
  - `jefatura_urologia_crossflow.spec.ts`

## Alcance del perfil
- Incluye:
  - perfiles de residentes
  - métricas longitudinales
  - interconectividad con `Central`
  - auth y `api_v1`
- Excluye:
  - quirófano clínico general
  - jefatura de quirófano
  - hospitalización
  - consulta
  - expediente

## Siguiente corte natural
- Si este perfil pasa en PostgreSQL staging, el siguiente paso natural es consolidar un perfil combinado de `jefaturas` o preparar el primer piloto supervisado con perfiles internos ya endurecidos.
