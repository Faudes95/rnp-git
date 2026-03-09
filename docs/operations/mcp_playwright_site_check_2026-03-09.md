# Verificación MCP Playwright del sitio `pilot_urologia`

- Fecha/hora de verificación: 2026-03-09 05:23-05:30 UTC aproximado
- Entorno probado: `APP_BOOT_PROFILE=pilot_urologia`
- URL base probada: `http://127.0.0.1:8012`
- Método: navegación real con el servidor MCP de Playwright usando auth básica embebida

## Páginas recorridas

| URL | Resultado | Observaciones |
|---|---|---|
| `/quirofano` | Carga correcta | Título `Módulo Quirúrgico - UROMED Urología`. Accesos a urgencias, programada, lista de espera y jefatura visibles. |
| `/quirofano/jefatura` | Carga correcta | Título `Jefatura de Quirófano \| UROMED`. Shell, navegación y KPIs visibles. |
| `/jefatura-urologia` | Carga correcta | Título `Jefatura de Urología`. Entradas a Central, Programa Académico, ANUER y Distribución de Salas visibles. |
| `/jefatura-urologia/central` | Carga correcta | Título `Central \| Jefatura de Urología`. KPIs, accesos a exámenes, casos, incidencias e insumos visibles. |
| `/jefatura-urologia/programa-academico/residentes/R5U_AVILA_CONTRERAS_O` | Carga correcta | Título `Avila Contreras O. \| Perfil de Residente`. Métricas longitudinales, evaluaciones, casos e incidencias visibles. |
| `/hospitalizacion/censo` | Carga correcta | Título `Censo Diario - Hospitalización`. Guardia, censo, calendario y CTA de ingreso visibles. |
| `/expediente` | Carga correcta | Título `Expediente Clinico Unico`. Buscador inteligente y accesos a contexto/captura visibles. |

## Hallazgos observados

### 1. Error menor de `favicon.ico` (corregido)
- En la primera pasada se observó:
  - `Failed to load resource: the server responded with a status of 404 (Not Found) @ /favicon.ico`
- Estado tras corrección:
  - resuelto
  - ya no aparecen errores de consola ni requests fallidos de favicon
- Acción aplicada:
  - se añadió una respuesta explícita de `/favicon.ico` redirigiendo al branding estático de UROMED

### 2. El perfil del residente parece abrir con el drawer visible (falso positivo de snapshot accesible)
- En la primera lectura MCP el panel `Editar perfil del residente` apareció listado en el árbol accesible.
- Verificación posterior por DOM/estilos:
  - `body.className = ""`
  - `aria-hidden = "true"`
  - `transform` del drawer fuera de pantalla
- Conclusión:
  - el drawer **no** estaba realmente abierto
  - el snapshot MCP lo listó porque el `<aside>` sigue presente en el DOM aunque esté oculto visualmente
- Acción aplicada:
  - se dejó un cierre defensivo del estado del drawer en frontend para evitar residuos visuales

### 3. Acumulación visible de datos E2E en Central y Residentes
- `Central` y el perfil del residente muestran múltiples evaluaciones/casos/incidencias E2E históricas.
- Impacto:
  - no rompe el flujo
  - sí ensucia lectura operativa en staging
- Recomendación:
  - definir política de limpieza o prefijo/rotación de fixtures E2E en staging

### 4. Importaciones recientes repetidas en Jefatura de Quirófano (corregido en portada)
- En la primera pasada se observaron tarjetas repetidas de `08-03-26.pdf` y `09-03-26.pdf`.
- Estado tras corrección:
  - resuelto en la portada `/quirofano/jefatura`
  - se muestra solo la última corrida visible por archivo/fecha
- Alcance de la corrección:
  - solo deduplicación visual en dashboard
  - el historial completo se conserva en `/quirofano/jefatura/importaciones`

### 5. Captura de screenshot por MCP no confiable en esta sesión
- La captura de screenshot por MCP falló con timeout esperando fuentes.
- Impacto:
  - no afecta el sitio
  - sí limita la recolección automática de evidencia visual desde esta MCP
- Recomendación:
  - usar snapshots MCP como evidencia primaria
  - o ajustar el método de captura visual fuera de la MCP si se necesita evidencia gráfica formal

## Conclusión

- El corredor `pilot_urologia` respondió correctamente en las páginas críticas revisadas.
- No se detectaron errores de carga bloqueantes en esta verificación manual con MCP.
- Tras corrección inmediata:
  - `favicon.ico` dejó de fallar
  - la portada de Jefatura de Quirófano ya no repite el mismo PDF en `Importaciones recientes`
  - el perfil del residente no abre realmente el drawer por defecto
- Los hallazgos observados son de tipo:
  - ruido visual / staging
  - UX menor
  - artefactos operativos
- No se observó una regresión clínica-operativa evidente en esta pasada de navegación real.
