# Runbook de Piloto Supervisado `pilot_urologia`

## Objetivo
- Operar el primer piloto interno supervisado usando `APP_BOOT_PROFILE=pilot_urologia`.
- Validar uso humano real sobre PostgreSQL staging sin abrir todavía el resto de perfiles generales.

## Commit seguro de referencia
- Perfil compuesto listo: `c2ee4554fce2afae6938272869eed0f9f2ea2ba7`
- Paquete operativo vigente: `231bc5b0c539220ea70d7d1d404d42c264632b45`

## Alcance del piloto
- Incluido:
  - quirófano urgencias/programada/lista de espera
  - jefatura de quirófano
  - jefatura urología
  - central
  - perfiles de residentes
  - hospitalización del corredor urológico
  - expediente clínico único en continuidad con hospitalización
- Excluido:
  - consulta general fuera del corredor urológico
  - hospitalización general fuera del corredor urológico
  - FHIR externo
  - IA asistencial
  - dashboards y reportes generales
  - investigación

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
export BASE_URL=http://127.0.0.1:8012
```

## Preflight obligatorio antes de abrir
1. Confirmar commit activo:
   - `git rev-parse HEAD`
   - debe corresponder al corte seguro aprobado del piloto
2. Confirmar estado de base:
   - `GET /status`
   - `GET /admin/database/status`
   - `GET /api/v1/database/status`
3. Confirmar plataforma de datos:
   - `target_mode=postgres`
   - `ready_for_target=true`
   - `pgvector_enabled=true`
   - `platform_assets_ready=true` salvo `partition_vitals` manual
4. Ejecutar preflight del corredor:
   - `npm run pilot:preflight:urologia`
5. Revisar artefactos:
   - `docs/executive_summary.md`
   - `docs/integration_findings.md`
   - `docs/recapture_opportunities.md`
   - `artifacts/censo_print_diff.json`
6. Generar paquete operativo reusable:
   - `npm run pilot:ops:pack:urologia`
   - salida en `artifacts/pilot_urologia/`

## Fases del piloto
### Fase 1: Dry run interno
- Script recomendado:
  - `npm run pilot:phase1:urologia`
- Objetivo:
  - validar arranque, navegación, exportes y corredor completo con equipo técnico/funcional
- Entregables:
  - `artifacts/pilot_urologia/phase1_dry_run_packet.md`
  - `artifacts/pilot_urologia/incident_log_template.md`
  - `artifacts/pilot_urologia/daily_summary_template.md`

### Fase 2: Piloto clínico supervisado
- Script recomendado:
  - `npm run pilot:phase2:urologia`
- Objetivo:
  - operar el corredor real con usuarios internos controlados durante la ventana de prueba
- Entregables:
  - `artifacts/pilot_urologia/phase2_supervised_packet.md`
  - actualización diaria de incidentes y resumen

### Fase 3: Cierre y decisión
- Script recomendado:
  - `npm run pilot:phase3:urologia`
- Objetivo:
  - consolidar evidencia y tomar decisión Go / No-Go
- Entregables:
  - `artifacts/pilot_urologia/phase3_closeout_packet.md`
  - `artifacts/pilot_urologia/go_no_go_template.md`

## Secuencia de apertura del piloto
1. Arrancar `pilot_urologia` sobre PostgreSQL staging.
2. Validar login con usuario operativo autorizado.
3. Validar navegación mínima:
   - `/quirofano`
   - `/quirofano/jefatura`
   - `/jefatura-urologia`
   - `/jefatura-urologia/central`
   - `/jefatura-urologia/programa-academico/residentes/R5U_AVILA_CONTRERAS_O`
   - `/hospitalizacion/censo`
   - `/expediente`
4. Hacer dry run funcional con equipo técnico antes de habilitar usuarios clínicos.
5. Abrir ventana supervisada a usuarios internos controlados.

## Usuarios recomendados del piloto
- 1 usuario de quirófano operativo
- 1 usuario de jefatura de quirófano
- 1 usuario de jefatura urología
- 1 usuario académico/residentes
- 1 usuario de hospitalización si participa en el corredor

## Criterios de aceptación del piloto
- Cero errores 500 en el corredor principal.
- Sin pérdida de contexto entre:
  - quirófano
  - postquirúrgica
  - hospitalización
  - expediente
  - perfil del residente
- Censo UI y exportación alineados.
- `Central` reflejando examen/caso/incidencia en perfil de residente.
- Auth, CSRF y endpoints admin protegidos.

## Registro de incidentes
- Registrar por cada incidencia:
  - fecha/hora
  - ruta
  - usuario/rol
  - pasos mínimos
  - severidad
  - screenshot si aplica
- Canal recomendado:
  - un único documento compartido de incidencias del piloto
  - y los hallazgos técnicos consolidados después en `docs/integration_findings.md`

## Rollback
- Si aparece fallo clínico-operativo severo y repetible:
  1. Suspender el piloto
  2. Detener `pilot_urologia`
  3. Volver al commit seguro inmediatamente anterior
- Puntos seguros actuales:
  - `22a01a6ac798c3a89ee4ca287bc5577446387967`
  - `c2ee4554fce2afae6938272869eed0f9f2ea2ba7`

## Siguiente paso si el piloto sale verde
- Integrar o abrir el siguiente corredor con mayor valor:
  - hospitalización más amplia
  - o jefaturas ampliadas
- No abrir todavía `consulta`, `expediente` o `investigacion` como perfiles operativos separados antes de cerrar este piloto.
