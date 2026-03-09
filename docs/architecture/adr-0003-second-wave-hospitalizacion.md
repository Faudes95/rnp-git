# ADR-0003: Segunda ola funcional de extracción, Hospitalización

## Estado
Aceptado

## Contexto
Después de sembrar perfiles/manifests y extraer la primera ola `Jefaturas + Quirófano`, la siguiente frontera crítica era `Hospitalización`, porque concentra:

- ingreso programado
- ingreso desde urgencias
- precheck e idempotencia
- guardia
- censo
- alta/egreso
- notas diarias y ward round

La lógica sigue siendo legacy, pero ya era necesario dejar una frontera de dominio para poder avanzar sin seguir ampliando el árbol plano de imports.

## Decisión
- Se crean fachadas de servicio:
  - `app/services/hospitalizacion/clinical.py`
  - `app/services/hospitalizacion/guardia.py`
  - `app/services/hospitalizacion/egreso.py`
  - `app/services/hospitalizacion/notes.py`
  - `app/services/hospitalizacion/ward.py`
- Se crea el wrapper HTTP:
  - `app/routers/hospitalizacion.py`
- `module_catalog` empieza a registrar estos módulos hospitalarios desde el wrapper:
  - `hospitalizacion`
  - `inpatient_notes`
  - `inpatient_labs_notes`
  - `inpatient_time_series`
  - `ward_smart`
  - `enfermeria`
- Los routers legacy principales consumen ya las fachadas agrupadas, sin cambiar rutas ni contratos.

## Consecuencias
- `Hospitalización` queda lista para la siguiente fase: mover reglas clínicas y exportes detrás de estas fachadas.
- El perfil interno `hospitalizacion` ya depende de una frontera visible de dominio.
- Se reduce la dispersión operativa de imports sin reescritura masiva.
