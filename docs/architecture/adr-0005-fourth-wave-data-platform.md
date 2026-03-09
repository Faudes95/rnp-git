# ADR-0005: Plataforma de datos PostgreSQL y assets SQL revisables

## Estado
Aceptado

## Contexto
Después de extraer `Jefaturas + Quirófano`, `Hospitalización` y `Consulta + Expediente`, el siguiente cuello de botella no está en routers sino en la plataforma de datos:

- `materialized_views.sql`
- `partition_vitals.sql`
- `migration_vector.sql`

Esos assets existían, pero no había:

- registro único de qué asset sirve para qué dominio
- revisión automatizable
- criterio homogéneo de aplicación
- health check que dijera si la plataforma está realmente lista para PostgreSQL

Sin esa capa, el cambio a Postgres en staging seguiría siendo informal y opaco.

## Decisión
Se introduce una capa `app/infra/db/` con tres piezas:

1. `sql_assets.py`
   - registro tipado de assets SQL
   - alcance por base (`clinical` / `surgical`)
   - modo objetivo (`postgres`)
   - prerequisitos y verificaciones

2. `health.py`
   - lectura de readiness de assets por DSN/engine
   - verificación de extensiones, relaciones, columnas e índices

3. `pipeline.py`
   - revisión (`review`)
   - aplicación controlada (`apply`)
   - separación entre assets autoaplicables y assets de revisión manual

Además:

- `app/services/db_platform_flow.py` expone `platform_assets_ready`
- el status administrativo ahora puede decir no solo si la base responde, sino si los assets SQL esperados ya están activos o siguen pendientes
- `partition_vitals.sql` queda marcado como `manual_review_required`, porque convertir una tabla base a particionada puede requerir una estrategia de migración dedicada y no debe autoaplicarse a ciegas

## Consecuencias
### Positivas
- El readiness hacia Postgres deja de ser implícito.
- Los assets SQL quedan ligados a dominios reales (`analytics`, `hospitalizacion`, `ia`).
- Se puede revisar y aplicar en staging sin meter lógica de plataforma en `main_full.py`.
- La CI puede validar la plataforma de datos sin tener que arrancar toda la app.

### Costes
- Se añade una capa más a mantener.
- `partition_vitals.sql` seguirá requiriendo una migración operacional explícita; el pipeline lo reporta, pero no lo resuelve mágicamente.

## Criterio de corte
- `platform_assets_ready` visible en status
- pipeline `review/apply` funcional
- workflow dedicado de revisión PostgreSQL
- tests unitarios de assets/health verdes
