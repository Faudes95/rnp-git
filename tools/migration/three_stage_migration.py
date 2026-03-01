#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple

from sqlalchemy import MetaData, create_engine, func, select, text
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url

try:
    import psycopg2
    from psycopg2 import sql as psycopg2_sql
except Exception:  # pragma: no cover - optional for non-Postgres usage
    psycopg2 = None
    psycopg2_sql = None

# Reuse existing SQLAlchemy models exactly as-is (additive migration tooling)
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))
from main import Base, SurgicalBase  # noqa: E402


def is_postgres_url(url: str) -> bool:
    return (url or "").startswith("postgresql")


def _normalize_psycopg2_dsn(url: str) -> str:
    return url.replace("postgresql+psycopg2://", "postgresql://")


def ensure_postgres_database(url: str) -> None:
    if not is_postgres_url(url):
        return
    if psycopg2 is None or psycopg2_sql is None:
        raise RuntimeError("psycopg2 no está disponible para crear la base de datos destino")
    parsed = make_url(url)
    db_name = parsed.database
    if not db_name:
        raise RuntimeError(f"URL sin nombre de base de datos: {url}")
    admin_url = str(parsed.set(database="postgres"))
    admin_url = _normalize_psycopg2_dsn(admin_url)
    conn = psycopg2.connect(admin_url)
    conn.autocommit = True
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
            exists = cur.fetchone() is not None
            if not exists:
                cur.execute(psycopg2_sql.SQL("CREATE DATABASE {}").format(psycopg2_sql.Identifier(db_name)))
    finally:
        conn.close()


def bootstrap_schema(target_url: str, kind: str) -> None:
    engine = create_engine(target_url)
    try:
        if kind == "clinical":
            Base.metadata.create_all(bind=engine)
        elif kind == "surgical":
            SurgicalBase.metadata.create_all(bind=engine)
        else:
            raise RuntimeError(f"Tipo de esquema desconocido: {kind}")
    finally:
        engine.dispose()


def _reflect_common_tables(source_engine: Engine, target_engine: Engine) -> Tuple[MetaData, MetaData, List[str]]:
    source_meta = MetaData()
    target_meta = MetaData()
    source_meta.reflect(bind=source_engine)
    target_meta.reflect(bind=target_engine)

    source_order = [table.name for table in source_meta.sorted_tables]
    target_names = set(target_meta.tables.keys())
    common = [name for name in source_order if name in target_names]
    return source_meta, target_meta, common


def _truncate_target_tables(target_engine: Engine, table_names: Iterable[str]) -> None:
    table_names = list(table_names)
    if not table_names:
        return
    with target_engine.begin() as conn:
        if target_engine.dialect.name == "postgresql":
            quoted = ", ".join(f'"{name}"' for name in table_names)
            conn.execute(text(f"TRUNCATE TABLE {quoted} RESTART IDENTITY CASCADE"))
        else:
            for name in reversed(table_names):
                conn.execute(text(f'DELETE FROM "{name}"'))


def copy_source_to_target(
    source_url: str,
    target_url: str,
    *,
    batch_size: int = 1000,
) -> Dict[str, int]:
    source_engine = create_engine(source_url)
    target_engine = create_engine(target_url)
    copied_counts: Dict[str, int] = {}

    try:
        source_meta, target_meta, common_tables = _reflect_common_tables(source_engine, target_engine)
        _truncate_target_tables(target_engine, common_tables)

        with source_engine.connect() as source_conn, target_engine.begin() as target_conn:
            for name in common_tables:
                source_table = source_meta.tables[name]
                target_table = target_meta.tables[name]
                result = source_conn.execute(select(source_table))
                total = 0
                while True:
                    chunk = result.fetchmany(batch_size)
                    if not chunk:
                        break
                    rows = [dict(row._mapping) for row in chunk]
                    target_conn.execute(target_table.insert(), rows)
                    total += len(rows)
                copied_counts[name] = total
    finally:
        source_engine.dispose()
        target_engine.dispose()

    return copied_counts


@dataclass
class ValidationRow:
    table: str
    source_count: int
    target_count: int

    @property
    def ok(self) -> bool:
        return self.source_count == self.target_count

    def to_dict(self) -> Dict[str, int | str | bool]:
        return {
            "table": self.table,
            "source_count": self.source_count,
            "target_count": self.target_count,
            "ok": self.ok,
        }


def validate_counts(source_url: str, target_url: str) -> List[ValidationRow]:
    source_engine = create_engine(source_url)
    target_engine = create_engine(target_url)
    rows: List[ValidationRow] = []
    try:
        source_meta, target_meta, common_tables = _reflect_common_tables(source_engine, target_engine)
        with source_engine.connect() as source_conn, target_engine.connect() as target_conn:
            for name in common_tables:
                source_table = source_meta.tables[name]
                target_table = target_meta.tables[name]
                source_count = int(source_conn.execute(select(func.count()).select_from(source_table)).scalar_one())
                target_count = int(target_conn.execute(select(func.count()).select_from(target_table)).scalar_one())
                rows.append(ValidationRow(name, source_count, target_count))
    finally:
        source_engine.dispose()
        target_engine.dispose()
    return rows


def write_json_report(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def stage_env_values(
    *,
    stage: str,
    sqlite_clinical_url: str,
    sqlite_surgical_url: str,
    pg_clinical_url: str,
    pg_surgical_url: str,
) -> Dict[str, str]:
    if stage == "shadow":
        return {
            "DB_MIGRATION_STAGE": "shadow",
            "DATABASE_URL": sqlite_clinical_url,
            "SURGICAL_DATABASE_URL": sqlite_surgical_url,
            "CLINICAL_SHADOW_DATABASE_URL": pg_clinical_url,
            "SURGICAL_SHADOW_DATABASE_URL": pg_surgical_url,
            "DB_DUAL_WRITE": "false",
        }
    if stage == "dual_write":
        return {
            "DB_MIGRATION_STAGE": "dual_write",
            "DATABASE_URL": sqlite_clinical_url,
            "SURGICAL_DATABASE_URL": sqlite_surgical_url,
            "CLINICAL_SHADOW_DATABASE_URL": pg_clinical_url,
            "SURGICAL_SHADOW_DATABASE_URL": pg_surgical_url,
            "DB_DUAL_WRITE": "true",
        }
    if stage == "cutover":
        # Keep rollback immediate by writing back to SQLite as shadow during stabilization.
        return {
            "DB_MIGRATION_STAGE": "cutover",
            "DATABASE_URL": pg_clinical_url,
            "SURGICAL_DATABASE_URL": pg_surgical_url,
            "CLINICAL_SHADOW_DATABASE_URL": sqlite_clinical_url,
            "SURGICAL_SHADOW_DATABASE_URL": sqlite_surgical_url,
            "DB_DUAL_WRITE": "true",
        }
    if stage == "finalize":
        return {
            "DB_MIGRATION_STAGE": "cutover",
            "DATABASE_URL": pg_clinical_url,
            "SURGICAL_DATABASE_URL": pg_surgical_url,
            "CLINICAL_SHADOW_DATABASE_URL": "",
            "SURGICAL_SHADOW_DATABASE_URL": "",
            "DB_DUAL_WRITE": "false",
        }
    if stage == "rollback":
        return {
            "DB_MIGRATION_STAGE": "rollback",
            "DATABASE_URL": sqlite_clinical_url,
            "SURGICAL_DATABASE_URL": sqlite_surgical_url,
            "CLINICAL_SHADOW_DATABASE_URL": pg_clinical_url,
            "SURGICAL_SHADOW_DATABASE_URL": pg_surgical_url,
            "DB_DUAL_WRITE": "true",
        }
    raise RuntimeError(f"Etapa inválida: {stage}")


def write_env_file(path: Path, values: Dict[str, str]) -> None:
    lines = [f'export {k}="{v}"' for k, v in values.items()]
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run_shadow(args) -> int:
    ensure_postgres_database(args.pg_clinical_url)
    ensure_postgres_database(args.pg_surgical_url)
    bootstrap_schema(args.pg_clinical_url, "clinical")
    bootstrap_schema(args.pg_surgical_url, "surgical")

    clinical_counts = copy_source_to_target(args.sqlite_clinical_url, args.pg_clinical_url)
    surgical_counts = copy_source_to_target(args.sqlite_surgical_url, args.pg_surgical_url)

    clinical_validation = [r.to_dict() for r in validate_counts(args.sqlite_clinical_url, args.pg_clinical_url)]
    surgical_validation = [r.to_dict() for r in validate_counts(args.sqlite_surgical_url, args.pg_surgical_url)]
    ok = all(r["ok"] for r in clinical_validation + surgical_validation)

    report = {
        "stage": "shadow",
        "timestamp": datetime.utcnow().isoformat(),
        "clinical_copied": clinical_counts,
        "surgical_copied": surgical_counts,
        "clinical_validation": clinical_validation,
        "surgical_validation": surgical_validation,
        "ok": ok,
    }
    write_json_report(Path(args.report), report)

    env_values = stage_env_values(
        stage="shadow",
        sqlite_clinical_url=args.sqlite_clinical_url,
        sqlite_surgical_url=args.sqlite_surgical_url,
        pg_clinical_url=args.pg_clinical_url,
        pg_surgical_url=args.pg_surgical_url,
    )
    write_env_file(Path(args.env_out), env_values)
    return 0 if ok else 2


def run_validate(args) -> int:
    clinical_validation = [r.to_dict() for r in validate_counts(args.sqlite_clinical_url, args.pg_clinical_url)]
    surgical_validation = [r.to_dict() for r in validate_counts(args.sqlite_surgical_url, args.pg_surgical_url)]
    ok = all(r["ok"] for r in clinical_validation + surgical_validation)
    report = {
        "stage": "validate",
        "timestamp": datetime.utcnow().isoformat(),
        "clinical_validation": clinical_validation,
        "surgical_validation": surgical_validation,
        "ok": ok,
    }
    write_json_report(Path(args.report), report)
    return 0 if ok else 2


def run_emit_env(args) -> int:
    values = stage_env_values(
        stage=args.stage,
        sqlite_clinical_url=args.sqlite_clinical_url,
        sqlite_surgical_url=args.sqlite_surgical_url,
        pg_clinical_url=args.pg_clinical_url,
        pg_surgical_url=args.pg_surgical_url,
    )
    env_out = Path(args.env_out)
    write_env_file(env_out, values)
    write_json_report(
        Path(args.report),
        {
            "stage": f"emit-env:{args.stage}",
            "timestamp": datetime.utcnow().isoformat(),
            "env_out": str(env_out),
            "values": values,
            "ok": True,
        },
    )
    return 0


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migración 3 etapas (shadow / dual-write / cutover) con rollback.")
    parser.add_argument("--sqlite-clinical-url", default=os.getenv("SQLITE_CLINICAL_URL", "sqlite:///./urologia.db"))
    parser.add_argument("--sqlite-surgical-url", default=os.getenv("SQLITE_SURGICAL_URL", "sqlite:///./urologia_quirurgico.db"))
    parser.add_argument("--pg-clinical-url", default=os.getenv("PG_CLINICAL_URL", ""))
    parser.add_argument("--pg-surgical-url", default=os.getenv("PG_SURGICAL_URL", ""))
    parser.add_argument("--report", default="backups/migration_reports/latest_report.json")
    parser.add_argument("--env-out", default="backups/migration_runtime.env")

    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("shadow", help="Etapa 1: shadow sync + validación + env shadow.")
    sub.add_parser("validate", help="Validación de paridad SQLite vs Postgres.")
    emit = sub.add_parser("emit-env", help="Genera archivo env para etapa específica.")
    emit.add_argument("--stage", required=True, choices=["shadow", "dual_write", "cutover", "finalize", "rollback"])
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    if not args.pg_clinical_url or not args.pg_surgical_url:
        print("ERROR: faltan --pg-clinical-url y/o --pg-surgical-url", file=sys.stderr)
        return 2
    if args.command == "shadow":
        return run_shadow(args)
    if args.command == "validate":
        return run_validate(args)
    if args.command == "emit-env":
        return run_emit_env(args)
    print(f"Comando no soportado: {args.command}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
