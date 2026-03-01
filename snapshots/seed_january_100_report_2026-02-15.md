# Validación Funcional - Carga Sintética Enero 2026

Tag de simulación: `SIM_ENE_2026`

## Carga ejecutada
- Consultas: 100
- Hospitalizaciones: 57
- Programaciones quirúrgicas: 64
- Notas postquirúrgicas: 23
- Programaciones de urgencia: 17
- Vitals: 57
- Labs: 342

## Desglose principal
- Sexo:
  - Masculino: 70
  - Femenino: 30
- Edad: promedio 52.1 años, mínimo 18, máximo 84
- Estatus de programación:
  - PROGRAMADA: 36
  - REALIZADA: 23
  - CANCELADA: 5
- Grupo de patología en programación:
  - ONCOLOGICO: 27
  - LITIASIS_URINARIA: 20
  - INFECCIOSO: 14
  - FUNCIONAL: 3

## Validación de rutas críticas (HTTP)
- `/` -> 200
- `/reporte` -> 200
- `/quirofano` -> 200
- `/hospitalizacion/guardia` -> 200
- `/api/stats/pendientes-programar/resumen` -> 200
- `/api/ai/quirofano/alertas` -> 200

## Notas
- Carga 100% aditiva sobre PostgreSQL, sin alterar rutas ni lógica clínica.
- Todos los datos de prueba quedaron marcados con `SIM_ENE_2026` para trazabilidad.
