# ADR-0002: Primera ola funcional de extracción, Jefaturas + Quirófano

## Estado
Aceptado

## Contexto
La refactorización incremental ya contaba con perfiles internos y manifests, pero los routers y flows de la primera ola seguían dependiendo del árbol plano legacy:

- `app/api/jefatura_web.py`
- `app/api/quirofano_jefatura_web.py`
- `app/api/urgencias.py`
- `app/api/legacy_web.py`

Para comenzar a trabajar por dominio sin romper el piloto, hacía falta introducir una capa intermedia real para:

- agrupar servicios de `Jefaturas`
- agrupar servicios de `Quirófano`
- registrar routers de la primera ola desde `app/routers`

## Decisión
- Se crean fachadas de servicio:
  - `app/services/jefaturas/central.py`
  - `app/services/jefaturas/urologia.py`
  - `app/services/quirofano/clinical.py`
  - `app/services/quirofano/jefatura.py`
- Se crean wrappers HTTP:
  - `app/routers/jefaturas.py`
  - `app/routers/quirofano.py`
- `module_catalog` registra la primera ola desde esos wrappers en lugar de apuntar directo al árbol `app/api`.
- Los routers legacy mantienen las mismas rutas y contratos, pero ahora importan sus casos de uso desde paquetes de dominio/servicio agrupados.

## Consecuencias
- `Jefaturas + Quirófano` ya tienen una primera frontera de dominio visible.
- La siguiente extracción podrá mover implementación detrás de esas fachadas sin reescribir routers ni tocar URLs.
- La dispersión de imports de primera ola disminuye y la arquitectura queda lista para dividir trabajo por sección.
