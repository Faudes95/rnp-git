# UROMED E2E

Suite E2E de UROMED basada en `@playwright/test` con cobertura por perfiles:

- `APP_BOOT_PROFILE=full`: validación integral de plataforma.
- `APP_BOOT_PROFILE=minimal_jefatura`: validación acotada a Jefatura de Quirófano.

Cobertura profunda adicional ya incluida:

- `Jefatura Urología`: perfiles de residentes, postquirúrgica, Central, exámenes, casos e incidencias.
- `Hospitalización`: ingreso por urgencias, ingreso programado, guardia, censo, precheck e idempotencia.
- `Quirófano`: urgencias, cirugía programada y lista de espera.
- `Expediente Clínico Único`: continuidad con hospitalización y nota diaria inpatient.
- `Jefatura de Quirófano`: plantillas, importaciones PDF, programación diaria y detalle del caso.

## Instalación

```bash
npm install
npx playwright install chromium
```

## Variables de entorno principales

- `BASE_URL` default: `http://127.0.0.1:8000`
- `AUTH_ENABLED` default: `true`
- `AUTH_USER` / `AUTH_PASS`
- fallback local: `IMSS_USER` / `IMSS_PASS`
- `APP_BOOT_PROFILE` default: `full`
- `STARTUP_INTERCONEXION_MODE` recomendado: `off`
- `AI_WARMUP_MODE` recomendado: `off`
- `DATABASE_URL`
- `SURGICAL_DATABASE_URL`
- opcionales:
  - `NGROK_URL`
  - `ENABLE_PII_ENCRYPTION`
  - `DATA_ENCRYPTION_KEY`
  - `PERF_P50_MS`
  - `PERF_P95_MS`
  - `PERF_CONCURRENCY`

## Fixtures incluidas

La suite ya versiona dos PDFs IMSS del layout validado para `Jefatura de Quirófano`:

- `e2e/fixtures/pdfs/08-03-26.pdf`
- `e2e/fixtures/pdfs/09-03-26.pdf`

Eso permite cubrir la carga asistida sin depender de rutas locales como `~/Downloads`.

## Ejecución local

Discovery de rutas:

```bash
npm run e2e:discovery
```

Smoke total:

```bash
npm run e2e:smoke
```

Módulos profundos:

```bash
npm run e2e:modules
```

Seguridad:

```bash
npm run e2e:security
```

Performance ligero:

```bash
npm run e2e:perf
```

Todo + reportes:

```bash
npm run e2e:all
```

Solo Jefatura de Quirófano profunda:

```bash
npx playwright test e2e/modules/jefatura/quirofano_jefatura.spec.ts
```

## Servicios externos y mocks

- FHIR: puede correrse contra runtime real o interceptarse vía `page.route()` / HAR replay.
- embeddings / IA: preferir flags de warmup apagado y validar estructura de respuesta antes que calidad semántica.
- email: stub o entorno local sin side effects.
- Redis/Celery: correr localmente si se quiere ejercitar cola; en smoke se tolera bypass si el runtime expone flags de desactivación.

## Artefactos

- `playwright-report/`
- `artifacts/results.xml`
- `artifacts/results.json`
- `artifacts/endpoints_matrix.json`
- `artifacts/perf_summary.json`
- `artifacts/censo_print_diff.json`
- `artifacts/recapture_matrix.json`
- `artifacts/har/`
- `artifacts/traces/`
- `artifacts/screenshots/`

Abrir el reporte HTML:

```bash
npx playwright show-report playwright-report
```
