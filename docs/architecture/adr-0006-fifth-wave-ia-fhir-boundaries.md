# ADR-0006: Boundaries explícitos para FHIR y FAU-BOT Core

## Estado
Aceptado

## Contexto
FHIR y `fau_bot_core` ya estaban activos, pero su contrato seguía implícito:

- FHIR dependía de rutas runtime sin una frontera formal más allá de `/api/fhir/*`
- `fau_bot_core` ya tenía DSNs read-only y schema de salida, pero el bridge HTTP del monolito no lo exponía como contrato verificable

En esta fase no conviene reescribirlos; conviene **cerrar su frontera** y convertirla en contrato observable.

## Decisión
- Crear `app/integrations/fhir/` como frontera explícita del bridge FHIR.
- Crear `app/integrations/fau_bot_core/` como frontera explícita del bridge IA.
- Mantener endpoints existentes; solo se añaden metadatos contractuales.
- Mover el registro HTTP de FHIR y IA a wrappers en `app/routers/`:
  - `app/routers/fhir.py`
  - `app/routers/ia.py`

## Contratos formales
### FHIR
- `GET /api/fhir/health`
- `GET /api/fhir/metadata`
- `GET /api/fhir/legacy-endpoints`
- `metadata` debe seguir emitiendo `CapabilityStatement` R4

### FAU-BOT Core
- lectura esperada por DSN read-only
- escritura aislada en `output_schema`
- scripts SQL read-only presentes y verificables
- bridge HTTP sigue siendo embebido, pero la frontera ya es visible en `boundary_contract`

## Consecuencias
### Positivas
- La suite y los operadores pueden inspeccionar la frontera FHIR/IA sin leer código interno.
- El router registry ya no apunta directo a `app.api` para estos dominios.
- Se deja lista la transición futura a un boundary más estricto por adaptadores/microservicio real.

### Costes
- Se añade otra capa de wrappers.
- El bridge sigue embebido; esta fase no externaliza `fau_bot_core`.

## Criterio de corte
- routers FHIR/IA envueltos en `app/routers`
- status de FAU core con `boundary_contract`
- metadata FHIR con contrato explícito
- tests de frontera verdes
