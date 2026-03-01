from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine

from .config import get_config


CFG = get_config()


def _schema() -> str:
    return CFG.output_schema


def ensure_output_schema(engine: Engine) -> None:
    schema = _schema()
    is_pg = str(engine.dialect.name).startswith("postgres")
    is_sqlite = str(engine.dialect.name).startswith("sqlite")
    pk = "BIGSERIAL PRIMARY KEY" if is_pg else "INTEGER PRIMARY KEY AUTOINCREMENT"

    with engine.begin() as conn:
        if is_pg:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
        elif is_sqlite:
            db_path = engine.url.database
            if db_path:
                rows = conn.execute(text("PRAGMA database_list")).fetchall()
                attached = {str(r[1]) for r in rows if len(r) >= 2}
                if schema not in attached:
                    conn.execute(text(f"ATTACH DATABASE :db_path AS {schema}"), {"db_path": db_path})

        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.runs (
                    id {pk},
                    triggered_by TEXT,
                    window_days INTEGER NOT NULL,
                    status TEXT NOT NULL,
                    started_at TIMESTAMP NOT NULL,
                    ended_at TIMESTAMP,
                    summary_json TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.reports (
                    id {pk},
                    run_id BIGINT,
                    agent_name TEXT,
                    scope TEXT,
                    report_date DATE,
                    metrics_json TEXT,
                    insights_json TEXT,
                    created_at TIMESTAMP NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.alerts (
                    id {pk},
                    run_id BIGINT,
                    title TEXT,
                    severity TEXT,
                    category TEXT,
                    description TEXT,
                    recommendation TEXT,
                    payload_json TEXT,
                    created_at TIMESTAMP NOT NULL,
                    acknowledged BOOLEAN DEFAULT FALSE,
                    resolved BOOLEAN DEFAULT FALSE
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.messages (
                    id {pk},
                    run_id BIGINT,
                    from_agent TEXT,
                    to_agent TEXT,
                    message_type TEXT,
                    severity TEXT,
                    payload_json TEXT,
                    created_at TIMESTAMP NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.connectors (
                    id {pk},
                    name TEXT UNIQUE,
                    display_name TEXT,
                    kind TEXT,
                    base_url TEXT,
                    auth_mode TEXT,
                    permissions_granted BOOLEAN DEFAULT FALSE,
                    enabled BOOLEAN DEFAULT FALSE,
                    config_json TEXT,
                    last_sync_at TIMESTAMP,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.external_events (
                    id {pk},
                    connector_name TEXT,
                    external_id TEXT,
                    event_date DATE,
                    event_type TEXT,
                    patient_ref TEXT,
                    payload_json TEXT,
                    created_at TIMESTAMP NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.knowledge_documents (
                    id {pk},
                    source TEXT,
                    title TEXT,
                    area TEXT,
                    content TEXT,
                    tags_json TEXT,
                    content_hash TEXT UNIQUE,
                    embedding_model TEXT,
                    embedding_dim INTEGER,
                    embedding_json TEXT,
                    active BOOLEAN DEFAULT TRUE,
                    updated_at TIMESTAMP NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.hitl_suggestions (
                    id {pk},
                    run_id BIGINT,
                    suggestion_type TEXT,
                    title TEXT,
                    recommendation TEXT,
                    evidence_json TEXT,
                    status TEXT NOT NULL DEFAULT 'PENDING_REVIEW',
                    reviewer TEXT,
                    reviewer_comment TEXT,
                    created_at TIMESTAMP NOT NULL,
                    reviewed_at TIMESTAMP
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.audit_log (
                    id {pk},
                    actor TEXT,
                    action TEXT,
                    target_type TEXT,
                    target_id TEXT,
                    details_json TEXT,
                    created_at TIMESTAMP NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.fau_engineering_issues (
                    id {pk},
                    source TEXT,
                    issue_code TEXT,
                    title TEXT,
                    category TEXT,
                    severity TEXT,
                    priority TEXT,
                    evidence_json TEXT,
                    first_seen TIMESTAMP NOT NULL,
                    last_seen TIMESTAMP NOT NULL,
                    status TEXT NOT NULL DEFAULT 'OPEN'
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.fau_pr_suggestions (
                    id {pk},
                    log_id BIGINT,
                    titulo_pr TEXT,
                    explicacion TEXT,
                    codigo_sugerido TEXT,
                    archivo_objetivo TEXT,
                    issue_id BIGINT,
                    title TEXT,
                    category TEXT,
                    priority TEXT,
                    risk_level TEXT,
                    status TEXT NOT NULL DEFAULT 'OPEN',
                    proposal_json TEXT,
                    patch_diff TEXT,
                    files_affected_json TEXT,
                    tests_required_json TEXT,
                    test_report_json TEXT,
                    builder TEXT,
                    creado_en TIMESTAMP,
                    aplicado_en TIMESTAMP,
                    created_at TIMESTAMP NOT NULL,
                    updated_at TIMESTAMP NOT NULL
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.fau_patch_runs (
                    id {pk},
                    pr_suggestion_id BIGINT,
                    builder TEXT,
                    started_at TIMESTAMP NOT NULL,
                    ended_at TIMESTAMP,
                    result TEXT,
                    error_json TEXT
                )
                """
            )
        )
        conn.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {schema}.fau_test_runs (
                    id {pk},
                    pr_suggestion_id BIGINT,
                    command_set TEXT,
                    stdout TEXT,
                    stderr TEXT,
                    result TEXT,
                    started_at TIMESTAMP NOT NULL,
                    ended_at TIMESTAMP
                )
                """
            )
        )

        # Compatibilidad aditiva para instalaciones previas con tablas parciales.
        if is_pg:
            conn.execute(text(f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN IF NOT EXISTS source TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN IF NOT EXISTS issue_code TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN IF NOT EXISTS title TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN IF NOT EXISTS category TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN IF NOT EXISTS severity TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN IF NOT EXISTS priority TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN IF NOT EXISTS evidence_json TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN IF NOT EXISTS first_seen TIMESTAMP"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN IF NOT EXISTS last_seen TIMESTAMP"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN IF NOT EXISTS status TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS log_id BIGINT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS titulo_pr TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS explicacion TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS codigo_sugerido TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS archivo_objetivo TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS issue_id BIGINT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS title TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS category TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS priority TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS risk_level TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS status TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS proposal_json TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS patch_diff TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS files_affected_json TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS tests_required_json TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS test_report_json TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS builder TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS creado_en TIMESTAMP"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS aplicado_en TIMESTAMP"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS created_at TIMESTAMP"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_patch_runs ADD COLUMN IF NOT EXISTS pr_suggestion_id BIGINT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_patch_runs ADD COLUMN IF NOT EXISTS builder TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_patch_runs ADD COLUMN IF NOT EXISTS started_at TIMESTAMP"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_patch_runs ADD COLUMN IF NOT EXISTS ended_at TIMESTAMP"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_patch_runs ADD COLUMN IF NOT EXISTS result TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_patch_runs ADD COLUMN IF NOT EXISTS error_json TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_test_runs ADD COLUMN IF NOT EXISTS pr_suggestion_id BIGINT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_test_runs ADD COLUMN IF NOT EXISTS command_set TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_test_runs ADD COLUMN IF NOT EXISTS stdout TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_test_runs ADD COLUMN IF NOT EXISTS stderr TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_test_runs ADD COLUMN IF NOT EXISTS result TEXT"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_test_runs ADD COLUMN IF NOT EXISTS started_at TIMESTAMP"))
            conn.execute(text(f"ALTER TABLE {schema}.fau_test_runs ADD COLUMN IF NOT EXISTS ended_at TIMESTAMP"))
        else:
            for stmt in [
                f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN source TEXT",
                f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN issue_code TEXT",
                f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN title TEXT",
                f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN category TEXT",
                f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN severity TEXT",
                f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN priority TEXT",
                f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN evidence_json TEXT",
                f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN first_seen TIMESTAMP",
                f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN last_seen TIMESTAMP",
                f"ALTER TABLE {schema}.fau_engineering_issues ADD COLUMN status TEXT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN log_id BIGINT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN titulo_pr TEXT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN explicacion TEXT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN codigo_sugerido TEXT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN archivo_objetivo TEXT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN issue_id BIGINT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN title TEXT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN category TEXT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN priority TEXT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN risk_level TEXT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN status TEXT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN proposal_json TEXT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN patch_diff TEXT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN files_affected_json TEXT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN tests_required_json TEXT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN test_report_json TEXT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN builder TEXT",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN creado_en TIMESTAMP",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN aplicado_en TIMESTAMP",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN created_at TIMESTAMP",
                f"ALTER TABLE {schema}.fau_pr_suggestions ADD COLUMN updated_at TIMESTAMP",
                f"ALTER TABLE {schema}.fau_patch_runs ADD COLUMN pr_suggestion_id BIGINT",
                f"ALTER TABLE {schema}.fau_patch_runs ADD COLUMN builder TEXT",
                f"ALTER TABLE {schema}.fau_patch_runs ADD COLUMN started_at TIMESTAMP",
                f"ALTER TABLE {schema}.fau_patch_runs ADD COLUMN ended_at TIMESTAMP",
                f"ALTER TABLE {schema}.fau_patch_runs ADD COLUMN result TEXT",
                f"ALTER TABLE {schema}.fau_patch_runs ADD COLUMN error_json TEXT",
                f"ALTER TABLE {schema}.fau_test_runs ADD COLUMN pr_suggestion_id BIGINT",
                f"ALTER TABLE {schema}.fau_test_runs ADD COLUMN command_set TEXT",
                f"ALTER TABLE {schema}.fau_test_runs ADD COLUMN stdout TEXT",
                f"ALTER TABLE {schema}.fau_test_runs ADD COLUMN stderr TEXT",
                f"ALTER TABLE {schema}.fau_test_runs ADD COLUMN result TEXT",
                f"ALTER TABLE {schema}.fau_test_runs ADD COLUMN started_at TIMESTAMP",
                f"ALTER TABLE {schema}.fau_test_runs ADD COLUMN ended_at TIMESTAMP",
            ]:
                try:
                    conn.execute(text(stmt))
                except Exception:
                    pass

        if is_pg:
            conn.execute(
                text(
                    f"ALTER TABLE {schema}.knowledge_documents "
                    "ADD COLUMN IF NOT EXISTS embedding_vector vector(384)"
                )
            )
            conn.execute(
                text(
                    f"CREATE INDEX IF NOT EXISTS ix_{schema}_knowledge_vector "
                    f"ON {schema}.knowledge_documents USING ivfflat (embedding_vector vector_cosine_ops) WITH (lists = 100)"
                )
            )

        if is_sqlite:
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS {schema}.ix_{schema}_runs_started ON runs(started_at)"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS {schema}.ix_{schema}_alerts_created ON alerts(created_at)"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS {schema}.ix_{schema}_hitl_status ON hitl_suggestions(status)"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS {schema}.ix_{schema}_eng_issues_status ON fau_engineering_issues(status)"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS {schema}.ix_{schema}_eng_issues_last_seen ON fau_engineering_issues(last_seen)"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS {schema}.ix_{schema}_pr_suggestions_status ON fau_pr_suggestions(status)"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS {schema}.ix_{schema}_pr_suggestions_updated ON fau_pr_suggestions(updated_at)"))
        else:
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_{schema}_runs_started ON {schema}.runs(started_at)"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_{schema}_alerts_created ON {schema}.alerts(created_at)"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_{schema}_hitl_status ON {schema}.hitl_suggestions(status)"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_{schema}_eng_issues_status ON {schema}.fau_engineering_issues(status)"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_{schema}_eng_issues_last_seen ON {schema}.fau_engineering_issues(last_seen)"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_{schema}_pr_suggestions_status ON {schema}.fau_pr_suggestions(status)"))
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS ix_{schema}_pr_suggestions_updated ON {schema}.fau_pr_suggestions(updated_at)"))


def schema_name() -> str:
    return _schema()
