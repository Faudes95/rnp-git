# Alembic Surgical

Migraciones separadas para la base quirúrgica (`SURGICAL_DATABASE_URL`) de forma aditiva.

## Flujo recomendado

1. Crear revisión inicial autogenerada:

```bash
alembic -c alembic_surgical.ini revision --autogenerate -m "initial surgical schema"
```

2. Aplicar migraciones:

```bash
alembic -c alembic_surgical.ini upgrade head
```

3. Activar ejecución opcional en startup:

```bash
export SURGICAL_ALEMBIC_ENABLED=true
export SURGICAL_ALEMBIC_CONFIG=alembic_surgical.ini
```

## Nota

El sistema mantiene compatibilidad con el flujo anterior (`ensure_surgical_schema`) como fallback.
