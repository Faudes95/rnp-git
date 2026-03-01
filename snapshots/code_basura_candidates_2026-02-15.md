# Candidatos de "código basura" (solo detección)

Análisis ejecutado con:
- `vulture` (alta confianza)
- `pyflakes`

## Candidatos seguros (alta probabilidad de eliminación sin impacto funcional)
1. `main.py:20`
- Import no usado: `BackgroundTasks`

2. `main.py:24`
- Import no usado: `Field`

3. `main.py:65`
- Import no usado: `svc_hospitalizacion_guardia_placeholder_flow`

4. `main.py:216`
- Parámetro no usado: `ttl` en `cache_patient(...)`

5. `app/services/hospitalizacion_flow.py:5`
- Import no usado: `math`

6. `app/services/hospitalizacion_flow.py:13`
- Import no usado: `and_`

7. `app/services/fau_hospitalizacion_agent.py:395`
- Variable asignada y no usada: `plt_vals`

## Candidatos con revisión previa (falsos positivos posibles)
- `main.py:24` `ValidationError`: pyflakes lo marca no usado localmente, pero sí se consume desde `app/services/consulta_flow.py` vía `import main as m` y `m.ValidationError`.
- Varios endpoints en `app/api/*` que vulture marca como "unused" por decorators FastAPI (no debe eliminarse sin validación manual).

## Recomendación
- Primera limpieza: aplicar solo los 7 candidatos "seguros".
- Después, correr smoke tests + snapshot de rutas para validar no-regresión.
