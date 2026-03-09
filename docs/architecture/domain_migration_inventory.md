# Inventario inicial de migración por dominios

| Módulo actual | Dominio destino | Dependencias a romper | Criticidad |
| --- | --- | --- | --- |
| `app/api/quirofano.py` | `quirofano` | `main_proxy`, helpers legacy, DB compartida | Alta |
| `app/api/quirofano_jefatura_web.py` | `jefaturas/quirofano` | `main_proxy`, servicios de jefatura quirúrgica | Alta |
| `app/api/jefatura_web.py` | `jefaturas/urologia` | `main_proxy`, perfiles de residentes, central | Alta |
| `app/api/hospitalizacion.py` | `hospitalizacion` | `main_proxy`, patient context, impresión/exportes | Alta |
| `app/api/expediente_plus.py` | `expediente` | `main_proxy`, búsqueda, identidad maestra | Alta |
| `app/api/consulta.py` + `consulta_externa.py` | `consulta` | `main_proxy`, validaciones clínicas, autofill | Alta |
| `app/api/reporte.py` + `reporte_stats.py` | `analytics` | datasets, data mart, materialized views | Media |
| `app/services/db_platform_flow.py` + SQL root scripts | `infra/db` | readiness de Postgres, pgvector, assets SQL, health checks | Alta |
| `app/api/fhir.py` | `fhir` | contratos externos, health, capability | Media |
| `app/api/fau_bot.py` + `fau_bot_core.py` | `ia` | boundary con `fau_bot_core`, acceso a DB | Media |
| `app/services/quirofano_flow.py` | `domain/quirofano` + `services/quirofano` | `main_proxy`, modelos quirúrgicos, waits | Alta |
| `app/services/hospitalizacion_flow.py` | `domain/hospitalizacion` + `services/hospitalizacion` | `main_proxy`, recaptura, guardia, censo | Alta |
| `app/services/consulta_externa_flow.py` | `domain/consulta` + `services/consulta` | `main_proxy`, patient context | Alta |
| `app/services/smart_expediente_flow.py` | `domain/expediente` | `main_proxy`, timeline clínico | Alta |
| `app/services/jefatura_central_flow.py` | `domain/jefaturas` | `main_proxy`, perfiles, exámenes, casos | Alta |
| `app/services/resident_profiles_flow.py` | `domain/jefaturas` | `main_proxy`, analítica quirúrgica | Alta |

## Primera ola propuesta
1. `Jefatura de Quirófano`
2. `Jefatura Urología / Central / Residentes`
3. `Quirófano urgencias/programada/lista de espera`

## Regla de avance
Ningún archivo nuevo dentro de `app/domain`, `app/infra`, `app/routers` o `app/integrations` puede depender de `main_full` o `main_proxy`.
