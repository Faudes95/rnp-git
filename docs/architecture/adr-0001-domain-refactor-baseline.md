# ADR-0001: Baseline de refactor incremental por dominios

## Estado
Aceptado

## Contexto
`main_full.py` sigue siendo el composition root principal de UROMED y concentra demasiado wiring de infraestructura, registro HTTP y dependencias de dominios clínicos. El repo ya tiene dos líneas seguras para refactorizar sin romper el piloto:

- baseline de suite/seguridad: `9b1750ed0b93c7727b9422b17a0b195a577da04e`
- baseline clínico validada end-to-end: `2a00e61477a2c06153ea11c979885bbf29abb14a`

Además, `main.py` ya resuelve entrypoints por perfil y existe `minimal_jefatura`, así que la evolución correcta es adelgazar el composition root y mover la composición interna por manifests, no reescribir la aplicación.

## Decisión
- `main.py` seguirá siendo el selector único de perfil.
- `main_full.py` se mantendrá temporalmente como composition root y se adelgazará por fases.
- Se crea una arquitectura destino con capas:
  - `app/infra`
  - `app/routers`
  - `app/services`
  - `app/domain`
  - `app/integrations`
- Se introduce un manifiesto por perfil con `entrypoint_module + active_modules`.
- Los perfiles nuevos nacen como internos de validación:
  - `consulta`
  - `hospitalizacion`
  - `quirofano`
  - `expediente`
  - `investigacion`
  - `jefatura_urologia`
  - `residentes_urologia`
- La primera ola funcional será `Jefaturas + Quirófano`.

## Reglas
- No cambiar rutas públicas en las primeras fases salvo decisión intencional documentada.
- No introducir imports nuevos hacia `main_full` o `app.core.app_context.main_proxy` fuera de compatibilidad legacy.
- El gate obligatorio por PR será:
  - snapshots de rutas/contratos sin drift no documentado
  - smoke E2E
  - P0/P1 del dominio tocado

## Consecuencias
- El refactor se puede ejecutar por sprints y por dominios sin freeze largo.
- Los perfiles nuevos podrán arrancarse internamente antes de abrirse a usuarios finales.
- La deuda legacy se deja encapsulada mientras se impide que siga creciendo.
