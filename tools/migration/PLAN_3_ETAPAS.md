# Plan De Migracion En 3 Etapas (Shadow / Dual-Write / Cutover)

Este plan es aditivo: no elimina rutas, no elimina campos, no altera la logica clinica.  
La app se cambia solo por variables de entorno para mantener rollback inmediato.

## Etapa 1: `shadow`
- Objetivo: crear/sincronizar PostgreSQL en paralelo sin tocar flujo productivo.
- Ejecucion:
```bash
cd "/Users/oscaralvarado/Documents/New project"
chmod +x tools/migration/run_three_stage.sh
tools/migration/run_three_stage.sh shadow
```
- Resultado esperado:
  - Copia completa `SQLite -> PostgreSQL` para BD clinica y quirurgica.
  - Reporte de paridad en `backups/migration_reports/shadow_*.json`.
  - Archivo `backups/migration_runtime.env` con `DB_MIGRATION_STAGE=shadow`.

Checklist de validacion (debe pasar todo):
- [ ] Sin errores de conexion a PostgreSQL.
- [ ] `ok: true` en reporte `shadow`.
- [ ] Conteo por tabla `source_count == target_count`.
- [ ] App sigue funcionando con SQLite como primaria.

## Etapa 2: `dual-write`
- Objetivo: seguir usando SQLite como primaria pero replicar cada commit en PostgreSQL.
- Ejecucion:
```bash
cd "/Users/oscaralvarado/Documents/New project"
tools/migration/run_three_stage.sh dual-write
set -a; source backups/migration_runtime.env; set +a
```
- Reiniciar app/worker con esas variables.

Checklist de validacion:
- [ ] Nuevos registros aparecen en SQLite y PostgreSQL.
- [ ] No cambia comportamiento HTTP (status, payload/HTML, redirecciones).
- [ ] Sin errores de integridad ni de tipos en logs.
- [ ] Validacion de paridad (`tools/migration/run_three_stage.sh validate`) sin mismatches.

## Etapa 3: `cutover`
- Objetivo: PostgreSQL pasa a primaria con rollback inmediato activo.
- Ejecucion:
```bash
cd "/Users/oscaralvarado/Documents/New project"
tools/migration/run_three_stage.sh cutover
set -a; source backups/migration_runtime.env; set +a
```
- En `cutover`, la primaria es PostgreSQL y SQLite queda como shadow de respaldo (`DB_DUAL_WRITE=true`).
- Se genera `backups/migration_runtime.rollback.env`.

Checklist de validacion:
- [ ] App inicia con `DATABASE_URL` y `SURGICAL_DATABASE_URL` en PostgreSQL.
- [ ] Rutas criticas responden igual que antes.
- [ ] Inserciones nuevas se reflejan en PostgreSQL y en SQLite shadow.
- [ ] `validate` sigue sin mismatches.

## Rollback inmediato
Si detectas cualquier incidencia, en el mismo host:
```bash
cd "/Users/oscaralvarado/Documents/New project"
set -a; source backups/migration_runtime.rollback.env; set +a
```
Reiniciar app/worker.  
Con esto regresas a SQLite primaria de inmediato, sin perder los nuevos writes del periodo de cutover (porque estaban en dual-write).

## Cierre de migracion (`finalize`)
Cuando cutover este estable:
```bash
cd "/Users/oscaralvarado/Documents/New project"
tools/migration/run_three_stage.sh finalize
set -a; source backups/migration_runtime.env; set +a
```
Esto deja PostgreSQL como primaria y desactiva shadow/dual-write.
