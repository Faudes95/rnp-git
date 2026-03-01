from __future__ import annotations
from app.core.time_utils import utcnow

import hashlib
import json
import math
import os
import re
import statistics
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    and_,
    desc,
    insert,
    inspect,
    select,
    text,
    update,
)
from sqlalchemy.orm import Session


FAU_CENTRAL_METADATA = MetaData()
JSON_SQL = Text().with_variant(Text(), "sqlite")

FAU_KNOWLEDGE_BASE = Table(
    "fau_knowledge_base",
    FAU_CENTRAL_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("fuente", String(255), index=True),
    Column("titulo", String(255), index=True, nullable=False),
    Column("area", String(120), index=True),
    Column("contenido", Text, nullable=False),
    Column("tags_json", JSON_SQL),
    Column("content_hash", String(64), unique=True, index=True, nullable=False),
    Column("embedding_model", String(120), index=True),
    Column("embedding_dim", Integer, index=True),
    Column("embedding_json", JSON_SQL),
    Column("activo", Boolean, default=True, index=True),
    Column("fecha_actualizacion", DateTime, default=utcnow, nullable=False, index=True),
)

FAU_SYSTEM_LOGS = Table(
    "fau_system_logs",
    FAU_CENTRAL_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("tipo_evento", String(60), index=True),
    Column("paciente_id", Integer, index=True),
    Column("contexto_json", JSON_SQL),
    Column("analisis_ia", Text),
    Column("aplicado", Boolean, default=False, index=True),
    Column("creado_en", DateTime, default=utcnow, nullable=False, index=True),
)

FAU_PR_SUGGESTIONS = Table(
    "fau_pr_suggestions",
    FAU_CENTRAL_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("log_id", Integer, index=True),
    Column("titulo_pr", String(240), index=True),
    Column("explicacion", Text),
    Column("codigo_sugerido", Text),
    Column("archivo_objetivo", String(255), index=True),
    Column("status", String(40), default="PENDING_REVIEW", index=True),
    Column("category", String(80), nullable=True, index=True),
    Column("priority", String(40), nullable=True, index=True),
    Column("proposal_json", Text, nullable=True),
    Column("patch_diff", Text, nullable=True),
    Column("test_report_json", Text, nullable=True),
    Column("files_affected_json", Text, nullable=True),
    Column("creado_en", DateTime, default=utcnow, nullable=False, index=True),
    Column("updated_en", DateTime, nullable=True, index=True),
    Column("aplicado_en", DateTime, nullable=True, index=True),
)

FAU_ENGINEERING_ISSUES = Table(
    "fau_engineering_issues",
    FAU_CENTRAL_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("source", String(80), index=True),
    Column("issue_code", String(120), index=True),
    Column("title", String(255), index=True),
    Column("category", String(80), index=True),
    Column("severity", String(20), index=True),
    Column("priority", String(20), index=True),
    Column("evidence_json", Text),
    Column("first_seen", DateTime, index=True),
    Column("last_seen", DateTime, index=True),
    Column("status", String(40), index=True),
    Column("created_at", DateTime, default=utcnow, nullable=False, index=True),
    Column("updated_at", DateTime, nullable=True, index=True),
)

PR_LOOP_STATUSES = {
    "OPEN",
    "SPEC_READY",
    "READY_FOR_PATCH",
    "PATCH_READY",
    "READY_FOR_TEST",
    "TEST_PASSED",
    "TEST_FAILED",
    "MERGED",
    "REJECTED",
}


def _safe_text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _safe_upper(value: Any) -> str:
    return _safe_text(value).upper()


def _dump(value: Any) -> str:
    try:
        return json.dumps(value, ensure_ascii=False)
    except Exception:
        return "{}"


def _load(value: Any, default: Any) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str):
        try:
            out = json.loads(value)
            if isinstance(out, type(default)):
                return out
        except Exception:
            return default
    return default


def _tokenize(text_value: str) -> List[str]:
    return [t for t in re.split(r"[^a-zA-Z0-9_áéíóúÁÉÍÓÚñÑ]+", text_value.lower()) if t]


def _cosine_similarity(a: List[float], b: List[float]) -> float:
    if not a or not b or len(a) != len(b):
        return 0.0
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(y * y for y in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return float(dot / (norm_a * norm_b))


def _fallback_embedding(text_value: str, dim: int = 384) -> List[float]:
    vec = [0.0] * dim
    for idx, token in enumerate(_tokenize(text_value)):
        h = int(hashlib.sha256(token.encode("utf-8")).hexdigest(), 16)
        pos = h % dim
        sign = -1.0 if (h % 2) else 1.0
        vec[pos] += sign * (1.0 + (idx % 3) * 0.1)
    norm = math.sqrt(sum(x * x for x in vec))
    if norm == 0:
        return vec
    return [x / norm for x in vec]


def _compute_embedding(text_value: str) -> Tuple[List[float], str]:
    try:
        from embeddings import compute_embedding

        out = compute_embedding(text_value)
        if out and isinstance(out, list):
            return [float(x) for x in out], "sentence-transformers"
    except Exception:
        pass
    return _fallback_embedding(text_value), "fallback-hash-embedding"


def _is_postgres(bind: Any) -> bool:
    try:
        return str(bind.dialect.name).startswith("postgres")
    except Exception:
        return False


def _vector_str(values: List[float]) -> str:
    return "[" + ",".join(f"{float(v):.8f}" for v in values) + "]"


def _column_exists(bind: Any, table_name: str, column_name: str) -> bool:
    try:
        cols = {str(c.get("name") or "") for c in inspect(bind).get_columns(table_name)}
        if column_name in cols:
            return True
    except Exception:
        pass
    try:
        if str(getattr(bind, "dialect", None).name).startswith("sqlite"):
            with bind.connect() as conn:
                rows = conn.execute(text(f"PRAGMA table_info({table_name})")).fetchall()
            return any(str(r[1]) == str(column_name) for r in rows)
    except Exception:
        pass
    try:
        with bind.connect() as conn:
            conn.execute(text(f"SELECT {column_name} FROM {table_name} WHERE 1=0"))
        return True
    except Exception:
        return False


def _ensure_add_column(bind: Any, table_name: str, column_name: str, ddl_type: str) -> None:
    if _column_exists(bind, table_name, column_name):
        return
    try:
        with bind.begin() as conn:
            conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {ddl_type}"))
    except Exception:
        # Revalidar para tolerar carreras entre procesos.
        if not _column_exists(bind, table_name, column_name):
            raise


def ensure_fau_pr_suggestions_schema(bind_or_session: Any) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    specs = [
        ("category", ["VARCHAR(80)", "TEXT"]),
        ("priority", ["VARCHAR(40)", "TEXT"]),
        ("proposal_json", ["TEXT"]),
        ("patch_diff", ["TEXT"]),
        ("test_report_json", ["TEXT"]),
        ("files_affected_json", ["TEXT"]),
        ("updated_en", ["TIMESTAMP", "DATETIME", "TEXT"]),
    ]
    for col_name, ddl_options in specs:
        if _column_exists(bind, "fau_pr_suggestions", col_name):
            continue
        for ddl in ddl_options:
            try:
                _ensure_add_column(bind, "fau_pr_suggestions", col_name, ddl)
            except Exception:
                continue
            if _column_exists(bind, "fau_pr_suggestions", col_name):
                break


def ensure_fau_engineering_issues_schema(bind_or_session: Any) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    specs = [
        ("source", ["VARCHAR(80)", "TEXT"]),
        ("issue_code", ["VARCHAR(120)", "TEXT"]),
        ("title", ["VARCHAR(255)", "TEXT"]),
        ("category", ["VARCHAR(80)", "TEXT"]),
        ("severity", ["VARCHAR(20)", "TEXT"]),
        ("priority", ["VARCHAR(20)", "TEXT"]),
        ("evidence_json", ["TEXT"]),
        ("first_seen", ["TIMESTAMP", "DATETIME", "TEXT"]),
        ("last_seen", ["TIMESTAMP", "DATETIME", "TEXT"]),
        ("status", ["VARCHAR(40)", "TEXT"]),
        ("created_at", ["TIMESTAMP", "DATETIME", "TEXT"]),
        ("updated_at", ["TIMESTAMP", "DATETIME", "TEXT"]),
    ]
    for col_name, ddl_options in specs:
        if _column_exists(bind, "fau_engineering_issues", col_name):
            continue
        for ddl in ddl_options:
            try:
                _ensure_add_column(bind, "fau_engineering_issues", col_name, ddl)
            except Exception:
                continue
            if _column_exists(bind, "fau_engineering_issues", col_name):
                break


def ensure_fau_central_schema(bind_or_session: Any) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return

    FAU_CENTRAL_METADATA.create_all(bind=bind, checkfirst=True)
    ensure_fau_pr_suggestions_schema(bind)
    ensure_fau_engineering_issues_schema(bind)

    if not _is_postgres(bind):
        return

    try:
        with bind.begin() as conn:
            conn.execute(text("CREATE EXTENSION IF NOT EXISTS vector"))
    except Exception:
        return

    try:
        insp = inspect(bind)
        cols = {c["name"] for c in insp.get_columns("fau_knowledge_base")}
        with bind.begin() as conn:
            if "embedding_vector" not in cols:
                conn.execute(text("ALTER TABLE fau_knowledge_base ADD COLUMN embedding_vector vector(384)"))
            conn.execute(
                text(
                    "CREATE INDEX IF NOT EXISTS ix_fau_knowledge_base_embedding_vector "
                    "ON fau_knowledge_base USING ivfflat (embedding_vector vector_cosine_ops) WITH (lists = 100)"
                )
            )
    except Exception:
        # Mantener funcionamiento aunque pgvector no esté listo.
        pass


def upsert_engineering_issue(
    db: Session,
    *,
    source: str,
    issue_code: str,
    title: str,
    category: str = "platform",
    severity: str = "WARNING",
    priority: str = "P2",
    evidence: Optional[Dict[str, Any]] = None,
    status: str = "OPEN",
) -> Dict[str, Any]:
    ensure_fau_central_schema(db)
    code = _safe_upper(issue_code)
    if not code:
        raise ValueError("issue_code requerido")
    row = db.execute(
        select(FAU_ENGINEERING_ISSUES).where(FAU_ENGINEERING_ISSUES.c.issue_code == code).limit(1)
    ).mappings().first()
    now = utcnow()
    values = {
        "source": _safe_text(source) or "telemetry",
        "issue_code": code,
        "title": _safe_text(title) or code,
        "category": _safe_text(category) or "platform",
        "severity": _safe_upper(severity) or "WARNING",
        "priority": _safe_upper(priority) or "P2",
        "evidence_json": _dump(evidence or {}),
        "status": _safe_upper(status) or "OPEN",
        "updated_at": now,
    }
    if row:
        db.execute(
            update(FAU_ENGINEERING_ISSUES)
            .where(FAU_ENGINEERING_ISSUES.c.id == int(row["id"]))
            .values(**values, last_seen=now)
        )
        issue_id = int(row["id"])
    else:
        result = db.execute(
            insert(FAU_ENGINEERING_ISSUES).values(
                **values,
                first_seen=now,
                last_seen=now,
                created_at=now,
            )
        )
        issue_id = int(result.inserted_primary_key[0])
    db.commit()
    out = db.execute(select(FAU_ENGINEERING_ISSUES).where(FAU_ENGINEERING_ISSUES.c.id == issue_id)).mappings().first()
    if not out:
        raise ValueError("No fue posible guardar issue")
    return {
        "id": int(out["id"]),
        "source": out.get("source"),
        "issue_code": out.get("issue_code"),
        "title": out.get("title"),
        "category": out.get("category"),
        "severity": out.get("severity"),
        "priority": out.get("priority"),
        "status": out.get("status"),
        "evidence": _load(out.get("evidence_json"), {}),
        "first_seen": out.get("first_seen").isoformat() if out.get("first_seen") else None,
        "last_seen": out.get("last_seen").isoformat() if out.get("last_seen") else None,
    }


def list_engineering_issues(db: Session, *, status: Optional[str] = None, limit: int = 200) -> List[Dict[str, Any]]:
    ensure_fau_central_schema(db)
    q = select(FAU_ENGINEERING_ISSUES)
    if status:
        q = q.where(FAU_ENGINEERING_ISSUES.c.status == _safe_upper(status))
    rows = db.execute(q.order_by(desc(FAU_ENGINEERING_ISSUES.c.last_seen)).limit(max(1, min(int(limit or 200), 3000)))).mappings().all()
    out: List[Dict[str, Any]] = []
    for row in rows:
        out.append(
            {
                "id": int(row["id"]),
                "source": row.get("source"),
                "issue_code": row.get("issue_code"),
                "title": row.get("title"),
                "category": row.get("category"),
                "severity": row.get("severity"),
                "priority": row.get("priority"),
                "status": row.get("status"),
                "evidence": _load(row.get("evidence_json"), {}),
                "first_seen": row.get("first_seen").isoformat() if row.get("first_seen") else None,
                "last_seen": row.get("last_seen").isoformat() if row.get("last_seen") else None,
            }
        )
    return out


def upsert_knowledge_document(
    db: Session,
    *,
    fuente: str,
    titulo: str,
    contenido: str,
    area: str = "GENERAL",
    tags: Optional[List[str]] = None,
) -> Dict[str, Any]:
    ensure_fau_central_schema(db)

    content = _safe_text(contenido)
    if not content:
        raise ValueError("contenido requerido")

    content_hash = hashlib.sha256(content.encode("utf-8")).hexdigest()
    emb, emb_model = _compute_embedding(content)

    row = db.execute(select(FAU_KNOWLEDGE_BASE).where(FAU_KNOWLEDGE_BASE.c.content_hash == content_hash).limit(1)).mappings().first()
    values = {
        "fuente": _safe_text(fuente),
        "titulo": _safe_text(titulo) or "Documento sin titulo",
        "area": _safe_upper(area or "GENERAL"),
        "contenido": content,
        "tags_json": _dump(tags or []),
        "embedding_model": emb_model,
        "embedding_dim": len(emb),
        "embedding_json": _dump(emb),
        "activo": True,
        "fecha_actualizacion": utcnow(),
    }

    if row:
        db.execute(update(FAU_KNOWLEDGE_BASE).where(FAU_KNOWLEDGE_BASE.c.id == row["id"]).values(**values))
        kb_id = int(row["id"])
    else:
        values["content_hash"] = content_hash
        result = db.execute(insert(FAU_KNOWLEDGE_BASE).values(**values))
        kb_id = int(result.inserted_primary_key[0])

    bind = getattr(db, "bind", None)
    if bind is not None and _is_postgres(bind) and len(emb) == 384:
        try:
            db.execute(
                text("UPDATE fau_knowledge_base SET embedding_vector = CAST(:v AS vector) WHERE id = :id"),
                {"v": _vector_str(emb), "id": kb_id},
            )
        except Exception:
            pass

    db.commit()
    return {
        "id": kb_id,
        "fuente": values["fuente"],
        "titulo": values["titulo"],
        "area": values["area"],
        "embedding_model": emb_model,
        "embedding_dim": len(emb),
    }


def search_knowledge(
    db: Session,
    *,
    query_text: str,
    area: Optional[str] = None,
    limit: int = 10,
) -> List[Dict[str, Any]]:
    ensure_fau_central_schema(db)
    q_text = _safe_text(query_text)
    if not q_text:
        return []

    q_emb, _ = _compute_embedding(q_text)
    q_tokens = set(_tokenize(q_text))

    q = select(FAU_KNOWLEDGE_BASE).where(FAU_KNOWLEDGE_BASE.c.activo.is_(True))
    if area:
        q = q.where(FAU_KNOWLEDGE_BASE.c.area == _safe_upper(area))

    rows = db.execute(q).mappings().all()
    scored: List[Tuple[float, Dict[str, Any]]] = []

    for r in rows:
        content = _safe_text(r.get("contenido"))
        emb = _load(r.get("embedding_json"), [])
        score_vec = _cosine_similarity(q_emb, emb) if isinstance(emb, list) and emb else 0.0

        tokens_doc = set(_tokenize(content[:3000]))
        overlap = len(q_tokens.intersection(tokens_doc))
        denom = len(q_tokens) if q_tokens else 1
        score_kw = float(overlap) / float(denom)

        score = (0.75 * score_vec) + (0.25 * score_kw)
        scored.append(
            (
                score,
                {
                    "id": r["id"],
                    "fuente": r["fuente"],
                    "titulo": r["titulo"],
                    "area": r["area"],
                    "score": round(score, 5),
                    "preview": content[:360],
                    "fecha_actualizacion": r["fecha_actualizacion"].isoformat() if r["fecha_actualizacion"] else None,
                },
            )
        )

    scored.sort(key=lambda t: t[0], reverse=True)
    return [x[1] for x in scored[: max(1, min(limit, 100))]]


def _latest_lab_by_marker(db: Session, main_module: Any, consulta_id: int) -> Dict[str, float]:
    rows = (
        db.query(main_module.LabDB)
        .filter(main_module.LabDB.consulta_id == consulta_id)
        .order_by(main_module.LabDB.timestamp.desc())
        .limit(300)
        .all()
    )
    out: Dict[str, float] = {}
    for r in rows:
        joined = f"{_safe_text(r.test_name)} {_safe_text(r.test_code)}".lower()
        marker = None
        if "creatin" in joined or joined.strip() in {"cr", "creat"}:
            marker = "creatinina"
        elif "hemoglob" in joined or "hgb" in joined or joined.strip().startswith("hb"):
            marker = "hemoglobina"
        elif "leuco" in joined or "wbc" in joined:
            marker = "leucocitos"
        elif "plaquet" in joined or "plt" in joined:
            marker = "plaquetas"
        elif "sodio" in joined or joined.strip() in {"na"}:
            marker = "sodio"
        elif "potasio" in joined or joined.strip() in {"k"}:
            marker = "potasio"
        if not marker or marker in out:
            continue
        val = _extract_float(r.value)
        if val is not None:
            out[marker] = val
    return out


def _extract_float(raw: Any) -> Optional[float]:
    txt = _safe_text(raw).replace(",", "")
    if not txt:
        return None
    out = ""
    dot = False
    minus = False
    for ch in txt:
        if ch.isdigit():
            out += ch
        elif ch == "." and not dot:
            out += ch
            dot = True
        elif ch == "-" and not minus and not out:
            out += ch
            minus = True
    if out in {"", "-", ".", "-."}:
        return None
    try:
        return float(out)
    except Exception:
        return None


def generate_patient_integral_report(
    db: Session,
    sdb: Session,
    main_module: Any,
    *,
    consulta_id: int,
) -> Dict[str, Any]:
    ensure_fau_central_schema(db)

    consulta = db.query(main_module.ConsultaDB).filter(main_module.ConsultaDB.id == int(consulta_id)).first()
    if consulta is None:
        raise ValueError("consulta no encontrada")

    hospitalizaciones = db.query(main_module.HospitalizacionDB).filter(main_module.HospitalizacionDB.consulta_id == int(consulta_id)).all()
    qx_programadas = sdb.query(main_module.SurgicalProgramacionDB).filter(main_module.SurgicalProgramacionDB.consulta_id == int(consulta_id)).all()
    qx_urgencias = sdb.query(main_module.SurgicalUrgenciaProgramacionDB).filter(main_module.SurgicalUrgenciaProgramacionDB.consulta_id == int(consulta_id)).all()
    postqx = sdb.query(main_module.SurgicalPostquirurgicaDB).filter(main_module.SurgicalPostquirurgicaDB.consulta_id == int(consulta_id)).all()

    labs = _latest_lab_by_marker(db, main_module, int(consulta_id))

    ecog_vals = []
    charlson_vals = []
    for q in qx_programadas:
        try:
            if q.ecog:
                ecog_vals.append(int(_extract_float(q.ecog) or 0))
        except Exception:
            pass
        try:
            if q.charlson:
                charlson_vals.append(int(_extract_float(q.charlson) or 0))
        except Exception:
            pass

    risk_score = 0.05
    factors = []

    if (consulta.edad or 0) >= 70:
        risk_score += 0.08
        factors.append("Edad >= 70")
    if qx_urgencias:
        risk_score += 0.10
        factors.append("Antecedente quirurgico de urgencia")
    if ecog_vals and max(ecog_vals) >= 2:
        risk_score += 0.06
        factors.append("ECOG >= 2")
    if charlson_vals and max(charlson_vals) >= 3:
        risk_score += 0.08
        factors.append("Charlson >= 3")
    if labs.get("creatinina", 0) >= 2.0:
        risk_score += 0.10
        factors.append("Creatinina >= 2")
    if labs.get("hemoglobina", 99) < 8.0:
        risk_score += 0.08
        factors.append("Hemoglobina < 8")
    if labs.get("leucocitos", 0) > 10000:
        risk_score += 0.05
        factors.append("Leucocitos > 10000")

    risk_score = max(0.01, min(0.98, risk_score))

    guideline_hits = search_knowledge(
        db,
        query_text=f"{_safe_text(consulta.diagnostico_principal)} urologia manejo riesgo",
        area="UROLOGIA",
        limit=5,
    )

    report_text = (
        f"Paciente {_safe_text(consulta.nombre)} (NSS {_safe_text(consulta.nss)}), diagnóstico principal: "
        f"{_safe_text(consulta.diagnostico_principal)}. Riesgo integral estimado: {round(risk_score * 100, 2)}%. "
        f"Hospitalizaciones: {len(hospitalizaciones)}, cirugías programadas: {len(qx_programadas)}, "
        f"urgencias: {len(qx_urgencias)}, notas postquirúrgicas: {len(postqx)}. "
        f"Factores de riesgo: {', '.join(factors) if factors else 'sin factores críticos detectados'}."
    )

    contexto = {
        "consulta_id": int(consulta_id),
        "paciente": {
            "nombre": consulta.nombre,
            "nss": consulta.nss,
            "curp": consulta.curp,
            "edad": consulta.edad,
            "sexo": consulta.sexo,
            "diagnostico_principal": consulta.diagnostico_principal,
        },
        "conteos": {
            "hospitalizaciones": len(hospitalizaciones),
            "qx_programadas": len(qx_programadas),
            "qx_urgencias": len(qx_urgencias),
            "postquirurgicas": len(postqx),
        },
        "laboratorios": labs,
        "prediccion": {
            "risk_score": round(risk_score, 4),
            "risk_level": "ALTO" if risk_score >= 0.7 else "MEDIO" if risk_score >= 0.4 else "BAJO",
            "factores": factors,
        },
        "evidencia": guideline_hits,
    }

    log_res = db.execute(
        insert(FAU_SYSTEM_LOGS).values(
            tipo_evento="CLINICO",
            paciente_id=int(consulta_id),
            contexto_json=_dump(contexto),
            analisis_ia=report_text,
            aplicado=False,
            creado_en=utcnow(),
        )
    )
    db.commit()

    return {
        "log_id": int(log_res.inserted_primary_key[0]),
        "report_text": report_text,
        "context": contexto,
    }


def analyze_runtime_log_and_suggest_pr(
    db: Session,
    *,
    source_file: str = "/tmp/rnp_uvicorn.err.log",
    max_lines: int = 400,
) -> Dict[str, Any]:
    ensure_fau_central_schema(db)

    raw = ""
    try:
        if os.path.isfile(source_file):
            with open(source_file, "r", encoding="utf-8", errors="ignore") as fh:
                lines = fh.readlines()
                raw = "".join(lines[-max_lines:])
    except Exception:
        raw = ""

    if not raw:
        raw = "Sin errores recientes en log o no disponible."

    findings: List[Dict[str, Any]] = []

    patterns = [
        (
            r"TypeError",
            "P2 - Fortalecer validación de tipos",
            "Se observó TypeError recurrente. Recomendado: validación temprana de payload y tipado defensivo en endpoint/servicio afectado.",
            "Agregar normalización/validación en capa API antes de invocar lógica de negocio.",
        ),
        (
            r"OperationalError|could not connect|connection refused",
            "P1 - Robustecer conectividad DB",
            "Se detectó error de conexión DB. Recomendado: retry con backoff y health-check preventivo.",
            "Agregar check de conectividad y manejo de reconexión en startup/task scheduler.",
        ),
        (
            r"Timeout|timed out",
            "P2 - Optimización de timeout",
            "Se detectaron timeouts. Recomendado: revisar queries sin índices y tiempos de worker.",
            "Agregar índices y paginación en rutas de alto volumen.",
        ),
        (
            r"No module named",
            "P2 - Dependencia faltante",
            "Hay dependencia faltante en runtime.",
            "Instalar paquete faltante o proteger import opcional con fallback.",
        ),
    ]

    for regex, title, explanation, patch in patterns:
        if re.search(regex, raw, flags=re.IGNORECASE):
            findings.append(
                {
                    "titulo_pr": title,
                    "explicacion": explanation,
                    "codigo_sugerido": patch,
                }
            )

    if not findings:
        findings.append(
            {
                "titulo_pr": "P3 - Hardening preventivo",
                "explicacion": "No se detectaron errores críticos recientes. Se sugiere hardening preventivo y pruebas de regresión.",
                "codigo_sugerido": "Agregar prueba de humo para rutas críticas y monitoreo de latencia por endpoint.",
            }
        )

    log_payload = {
        "source_file": source_file,
        "sample_lines": raw[-5000:],
        "findings_count": len(findings),
    }

    log_res = db.execute(
        insert(FAU_SYSTEM_LOGS).values(
            tipo_evento="ERROR_CODIGO",
            paciente_id=None,
            contexto_json=_dump(log_payload),
            analisis_ia="Analisis de logs completado por fau_BOT_Dev con sugerencias HITL.",
            aplicado=False,
            creado_en=utcnow(),
        )
    )
    log_id = int(log_res.inserted_primary_key[0])

    created_ids = []
    for f in findings:
        pr = db.execute(
            insert(FAU_PR_SUGGESTIONS).values(
                log_id=log_id,
                titulo_pr=f["titulo_pr"],
                explicacion=f["explicacion"],
                codigo_sugerido=f["codigo_sugerido"],
                archivo_objetivo="main.py",
                status="PENDING_REVIEW",
                creado_en=utcnow(),
            )
        )
        created_ids.append(int(pr.inserted_primary_key[0]))

    db.execute(
        insert(FAU_SYSTEM_LOGS).values(
            tipo_evento="SUGERENCIA_PR",
            paciente_id=None,
            contexto_json=_dump({"pr_ids": created_ids}),
            analisis_ia="Se generaron sugerencias de PR para revisión humana (HITL).",
            aplicado=False,
            creado_en=utcnow(),
        )
    )

    db.commit()

    return {
        "log_id": log_id,
        "findings": findings,
        "pr_suggestion_ids": created_ids,
    }


def list_system_logs(db: Session, *, event_type: Optional[str] = None, limit: int = 120) -> List[Dict[str, Any]]:
    ensure_fau_central_schema(db)
    q = select(FAU_SYSTEM_LOGS)
    if event_type:
        q = q.where(FAU_SYSTEM_LOGS.c.tipo_evento == _safe_upper(event_type))
    rows = db.execute(q.order_by(desc(FAU_SYSTEM_LOGS.c.id)).limit(max(1, min(limit, 1000)))).mappings().all()
    return [
        {
            "id": r["id"],
            "tipo_evento": r["tipo_evento"],
            "paciente_id": r["paciente_id"],
            "contexto": _load(r["contexto_json"], {}),
            "analisis_ia": r["analisis_ia"],
            "aplicado": bool(r["aplicado"]),
            "creado_en": r["creado_en"].isoformat() if r["creado_en"] else None,
        }
        for r in rows
    ]


def list_pr_suggestions(db: Session, *, status: Optional[str] = None, limit: int = 120) -> List[Dict[str, Any]]:
    ensure_fau_central_schema(db)
    q = select(FAU_PR_SUGGESTIONS)
    if status:
        q = q.where(FAU_PR_SUGGESTIONS.c.status == _safe_upper(status))
    rows = db.execute(q.order_by(desc(FAU_PR_SUGGESTIONS.c.id)).limit(max(1, min(limit, 1000)))).mappings().all()
    return [
        {
            "id": r["id"],
            "log_id": r["log_id"],
            "titulo_pr": r["titulo_pr"],
            "explicacion": r["explicacion"],
            "codigo_sugerido": r["codigo_sugerido"],
            "archivo_objetivo": r["archivo_objetivo"],
            "status": r["status"],
            "category": r.get("category"),
            "priority": r.get("priority"),
            "proposal_json": _load(r.get("proposal_json"), {}),
            "patch_diff": r.get("patch_diff") or "",
            "test_report_json": _load(r.get("test_report_json"), {}),
            "files_affected_json": _load(r.get("files_affected_json"), []),
            "creado_en": r["creado_en"].isoformat() if r["creado_en"] else None,
            "updated_en": r.get("updated_en").isoformat() if r.get("updated_en") else None,
            "aplicado_en": r["aplicado_en"].isoformat() if r["aplicado_en"] else None,
        }
        for r in rows
    ]


def set_pr_suggestion_status(db: Session, suggestion_id: int, status: str) -> Dict[str, Any]:
    ensure_fau_central_schema(db)
    new_status = _safe_upper(status or "PENDING_REVIEW")
    values = {"status": new_status, "updated_en": utcnow()}
    if new_status == "APPLIED":
        values["aplicado_en"] = utcnow()

    db.execute(update(FAU_PR_SUGGESTIONS).where(FAU_PR_SUGGESTIONS.c.id == int(suggestion_id)).values(**values))
    db.commit()

    row = db.execute(select(FAU_PR_SUGGESTIONS).where(FAU_PR_SUGGESTIONS.c.id == int(suggestion_id))).mappings().first()
    if not row:
        raise ValueError("Sugerencia no encontrada")

    return {
        "id": row["id"],
        "status": row["status"],
        "updated_en": row.get("updated_en").isoformat() if row.get("updated_en") else None,
        "aplicado_en": row["aplicado_en"].isoformat() if row["aplicado_en"] else None,
    }


def get_pr_suggestion(db: Session, suggestion_id: int) -> Dict[str, Any]:
    ensure_fau_central_schema(db)
    row = db.execute(
        select(FAU_PR_SUGGESTIONS).where(FAU_PR_SUGGESTIONS.c.id == int(suggestion_id)).limit(1)
    ).mappings().first()
    if not row:
        raise ValueError("Sugerencia no encontrada")
    return {
        "id": int(row["id"]),
        "log_id": row.get("log_id"),
        "titulo_pr": row.get("titulo_pr"),
        "explicacion": row.get("explicacion"),
        "codigo_sugerido": row.get("codigo_sugerido"),
        "archivo_objetivo": row.get("archivo_objetivo"),
        "status": row.get("status"),
        "category": row.get("category"),
        "priority": row.get("priority"),
        "proposal_json": _load(row.get("proposal_json"), {}),
        "patch_diff": row.get("patch_diff") or "",
        "test_report_json": _load(row.get("test_report_json"), {}),
        "files_affected_json": _load(row.get("files_affected_json"), []),
        "creado_en": row.get("creado_en").isoformat() if row.get("creado_en") else None,
        "updated_en": row.get("updated_en").isoformat() if row.get("updated_en") else None,
        "aplicado_en": row.get("aplicado_en").isoformat() if row.get("aplicado_en") else None,
    }


def set_pr_suggestion_spec(db: Session, suggestion_id: int, proposal_json: Dict[str, Any], status: str) -> Dict[str, Any]:
    ensure_fau_central_schema(db)
    new_status = _safe_upper(status or "SPEC_READY")
    db.execute(
        update(FAU_PR_SUGGESTIONS)
        .where(FAU_PR_SUGGESTIONS.c.id == int(suggestion_id))
        .values(
            proposal_json=_dump(proposal_json),
            status=new_status,
            updated_en=utcnow(),
        )
    )
    db.commit()
    return get_pr_suggestion(db, suggestion_id)


def set_pr_suggestion_patch(
    db: Session,
    suggestion_id: int,
    patch_diff: str,
    status: str,
    *,
    files_affected: Optional[List[str]] = None,
) -> Dict[str, Any]:
    ensure_fau_central_schema(db)
    new_status = _safe_upper(status or "PATCH_READY")
    values: Dict[str, Any] = {
        "patch_diff": patch_diff,
        "status": new_status,
        "updated_en": utcnow(),
    }
    if files_affected is not None:
        values["files_affected_json"] = _dump(files_affected)
    db.execute(update(FAU_PR_SUGGESTIONS).where(FAU_PR_SUGGESTIONS.c.id == int(suggestion_id)).values(**values))
    db.commit()
    return get_pr_suggestion(db, suggestion_id)


def set_pr_suggestion_test_report(db: Session, suggestion_id: int, test_report_json: Dict[str, Any], status: str) -> Dict[str, Any]:
    ensure_fau_central_schema(db)
    new_status = _safe_upper(status or "TEST_FAILED")
    db.execute(
        update(FAU_PR_SUGGESTIONS)
        .where(FAU_PR_SUGGESTIONS.c.id == int(suggestion_id))
        .values(
            test_report_json=_dump(test_report_json),
            status=new_status,
            updated_en=utcnow(),
        )
    )
    db.commit()
    return get_pr_suggestion(db, suggestion_id)
