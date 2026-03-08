# Hallazgos de integración

## Verde

- `postquirúrgica -> indexación -> perfil de residente` funciona y refleja actividad longitudinal.
- `Central -> exámenes/casos/incidencias -> perfil del residente` funciona y mantiene sincronía sin recaptura paralela.
- `cirugía programada -> hospitalización -> expediente clínico único` conserva continuidad y permite guardar nota diaria estructurada.
- `urgencias -> postquirúrgica -> hospitalización` ya funciona de extremo a extremo con prefill clínico reutilizable.
- `lista de espera de programación` acepta consulta existente sin recaptura externa.
- `guardia/censo -> exportación XLSX` ya vuelve a generar archivo descargable y conserva al paciente trazador tanto en UI como en export.

## Amarillo

- El diff bruto `ui_row_count` vs `exported_row_count` del censo sigue siendo diagnóstico y no debe leerse aún como equivalencia exacta fila por fila; la suite confirma presencia del paciente esperado, pero la comparación completa de conjuntos todavía puede endurecerse.

## Observaciones operativas

- El corredor de urgencias quedó resuelto por producto: el problema era colisión de `quirofano_id` sintético en `surgical_programaciones`.
- El exportador de censo quedó resuelto por producto: el problema estaba en celdas fusionadas de Excel (`MergedCell`) al reescribir la plantilla.
- La siguiente mejora útil es comparar de forma más estricta la población activa completa del censo contra la exportación XLSX y no solo verificar el paciente trazador.
