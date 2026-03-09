# Checklist de Aceptación del Piloto `pilot_urologia`

## Infraestructura
- [ ] `APP_BOOT_PROFILE=pilot_urologia`
- [ ] `DATABASE_PLATFORM_TARGET=postgres`
- [ ] `GET /status` responde `200`
- [ ] `GET /admin/database/status` responde con auth
- [ ] `GET /api/v1/database/status` reporta PostgreSQL
- [ ] `ready_for_target=true`
- [ ] `platform_assets_ready=true` con excepción documentada de `partition_vitals`

## Seguridad y gobernanza
- [ ] Auth básica operativa
- [ ] CSRF operativa en formularios críticos
- [ ] `/admin/database/status` protegida
- [ ] No hay endpoints admin inesperados expuestos
- [ ] Outbox/eventos/health sin regresión

## Corredor clínico
- [ ] urgencias quirúrgicas
- [ ] cirugía programada
- [ ] lista de espera
- [ ] jefatura de quirófano con importación PDF y conciliación
- [ ] `postquirúrgica -> perfil de residente`
- [ ] `Central -> examen -> perfil`
- [ ] `Central -> caso/incidencia -> perfil`
- [ ] hospitalización programada
- [ ] hospitalización desde urgencias
- [ ] guardia/censo
- [ ] expediente longitudinal

## Continuidad de contexto
- [ ] no se pierde `consulta_id`
- [ ] no se pierde `hospitalizacion_id`
- [ ] no hay recaptura innecesaria de identidad del paciente
- [ ] no hay recaptura innecesaria de diagnóstico ya conocido
- [ ] el residente refleja actividad y evaluación acumulada

## Evidencia operativa
- [ ] `npm run pilot:preflight:urologia` verde
- [ ] `docs/executive_summary.md` actualizado
- [ ] `docs/integration_findings.md` actualizado
- [ ] `docs/recapture_opportunities.md` actualizado
- [ ] `artifacts/censo_print_diff.json` sin discrepancias críticas

## Decisión
- [ ] Verde: abrir piloto supervisado
- [ ] Amarillo: abrir piloto con restricciones y lista corta de correcciones
- [ ] Rojo: no abrir, corregir y repetir preflight
