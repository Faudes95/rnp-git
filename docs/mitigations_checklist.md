# Checklist de mitigaciones

- [x] Corregir `POST /quirofano/urgencias/solicitud` para que no devuelva `500` con combinaciones válidas.
- [x] Corregir `_write_censo_excel` para que `/hospitalizacion/censo/imprimir` vuelva a generar XLSX válido.
- [x] Proteger `/admin/database/status` con auth.
- [x] Restaurar `admin/actualizar_data_mart` y rutas administrativas relacionadas en `full`.
- [x] Mantener `minimal_jefatura` como smoke operativo independiente para no bloquear validación de quirófano por fallos del resto de la plataforma.
- [x] Repetir y cerrar la corrida `full` después de corregir urgencias, censo y seguridad.
- [ ] Fortalecer la comparación estricta `UI censo vs XLSX vs fuente operativa` para equivalencia completa de filas, no solo del paciente trazador.
- [ ] Seguir reduciendo puntos de recaptura parcial en formularios que ya cuentan con `patient_context` o datos enlazados.
- [ ] Si se quiere experiencia cartográfica completa, instalar `folium`; mientras tanto el mapa queda en modo degradado seguro.
