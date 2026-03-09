# ADR-0004: Tercera ola funcional de extracción, Consulta + Expediente

## Estado
Aceptado

## Contexto
Con `Jefaturas + Quirófano` y `Hospitalización` ya encapsulados por fachadas y wrappers HTTP, faltaba cerrar el corredor longitudinal de:

- consulta
- consulta externa
- interconsultas
- expediente clínico único
- perfil clínico
- EHR integrado
- identidad maestra
- autollenado de paciente

Sin esa frontera, hospitalización y quirófano seguirían dependiendo de módulos dispersos para resolver contexto clínico.

## Decisión
- Se crea la fachada de consulta en:
  - `app/services/consulta_domain/`
- Se crea la fachada de expediente en:
  - `app/services/expediente/`
- Se crean wrappers HTTP:
  - `app/routers/consulta.py`
  - `app/routers/expediente.py`
- `module_catalog` registra ahora los módulos de consulta/expediente a través de esos wrappers.
- Los routers legacy principales de esta ola consumen ya las fachadas nuevas:
  - `app/api/consulta_externa.py`
  - `app/api/perfil_clinico.py`
  - `app/api/ehr_integrado.py`

## Consecuencias
- `Consulta + Expediente` ya forman una frontera visible de dominio.
- La siguiente fase podrá mover reglas de contexto y continuidad clínica detrás de estas fachadas sin tocar URLs.
- El árbol de arranque por perfiles ya refleja mejor la arquitectura destino completa.
