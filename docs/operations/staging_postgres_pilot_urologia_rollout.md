# Staging PostgreSQL para `APP_BOOT_PROFILE=pilot_urologia`

## Objetivo
- Validar el primer perfil operativo compuesto para el piloto urológico supervisado.
- Probar juntos `quirofano`, `jefatura_quirofano`, `jefatura_urologia`, `Central`, `perfiles de residentes`, el corredor mínimo de `hospitalizacion` y la continuidad en `expediente` sobre PostgreSQL real.

## Variables de entorno
```bash
export APP_BOOT_PROFILE=pilot_urologia
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
2. Verificar `GET /admin/database/status` y `GET /api/v1/database/status` con auth.
3. Reiniciar con `APP_BOOT_PROFILE=pilot_urologia`.
4. Confirmar `/status`.
5. Verificar que el manifest expone:
   - `quirofano`
   - `urgencias`
   - `jefatura_quirofano`
   - `jefaturas`
   - `hospitalizacion`
   - `consulta`
   - `expediente_plus`
   - `perfil_clinico`
   - `ehr_integrado`
   - `shell`
   - `auth_login`
   - `api_v1`
6. Ejecutar:
   - `npm run e2e:staging:pilot_urologia`

## Criterios de listo
- Motores clínico y quirúrgico en PostgreSQL.
- `platform_assets_ready=true` salvo `partition_vitals`, que se mantiene manual.
- Suite verde de:
  - `e2e/modules/quirofano/*`
  - `e2e/modules/jefatura/quirofano_jefatura.spec.ts`
  - `e2e/modules/jefatura/jefatura_central.spec.ts`
  - `e2e/modules/jefatura/jefatura_urologia_residentes.spec.ts`
  - `e2e/modules/jefatura/jefatura_urologia_crossflow.spec.ts`
  - `e2e/modules/hospitalizacion/*`
  - `e2e/modules/expediente/expediente_clinico_unico.spec.ts`

## Alcance del perfil
- Incluye:
  - quirófano clínico
  - urgencias quirúrgicas
  - lista de espera
  - jefatura de quirófano
  - jefatura urología
  - central
  - perfiles longitudinales de residentes
  - ingreso hospitalario, guardia y censo
  - núcleo mínimo de consulta para sembrar contexto (`consulta` + `patient_autofill`)
  - expediente clínico único y continuidad de nota diaria inpatient
  - auth, shell, `api_v1`, validación clínica y gobernanza
- Excluye:
  - FHIR
  - IA
  - dashboard y reportes generales

## Siguiente corte natural
- Si `pilot_urologia` queda verde en PostgreSQL staging con hospitalización y expediente integrados, el siguiente paso recomendado es consolidar un piloto supervisado real o decidir si se abre una ola equivalente para `consulta`/`hospitalizacion` fuera del corredor urológico.
