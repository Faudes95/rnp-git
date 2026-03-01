from __future__ import annotations
from app.core.time_utils import utcnow

import json
import math
import statistics
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import (
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    and_,
    desc,
    func,
    insert,
    select,
    update,
)
from sqlalchemy.orm import Session

from app.services.fau_hospitalizacion_agent import (
    analyze_hospitalizacion_patients,
    ensure_hospital_agent_schema,
)
from app.services.fau_quirofano_agent import (
    analyze_quirofano_programaciones,
    ensure_quirofano_agent_schema,
)

FAU_METADATA = MetaData()

FAU_AGENT_RUNS = Table(
    "fau_agent_runs",
    FAU_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("triggered_by", String(120), index=True),
    Column("window_days", Integer, default=30),
    Column("status", String(30), default="RUNNING", index=True),
    Column("started_at", DateTime, default=utcnow, nullable=False, index=True),
    Column("ended_at", DateTime, nullable=True, index=True),
    Column("summary_json", Text),
)

FAU_AGENT_REPORTS = Table(
    "fau_agent_reports",
    FAU_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, index=True, nullable=False),
    Column("agent_name", String(120), index=True, nullable=False),
    Column("scope", String(80), index=True),
    Column("report_date", Date, index=True),
    Column("metrics_json", Text),
    Column("insights_json", Text),
    Column("created_at", DateTime, default=utcnow, nullable=False, index=True),
)

FAU_AGENT_MESSAGES = Table(
    "fau_agent_messages",
    FAU_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, index=True, nullable=False),
    Column("from_agent", String(120), index=True, nullable=False),
    Column("to_agent", String(120), index=True, nullable=False),
    Column("message_type", String(80), index=True),
    Column("severity", String(30), index=True),
    Column("payload_json", Text),
    Column("created_at", DateTime, default=utcnow, nullable=False, index=True),
)

FAU_CENTRAL_ALERTS = Table(
    "fau_central_alerts",
    FAU_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, index=True),
    Column("title", String(220), index=True),
    Column("severity", String(30), index=True),
    Column("category", String(80), index=True),
    Column("description", Text),
    Column("recommendation", Text),
    Column("payload_json", Text),
    Column("created_at", DateTime, default=utcnow, nullable=False, index=True),
)

FAU_ACTION_PROPOSALS = Table(
    "fau_action_proposals",
    FAU_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, index=True),
    Column("source_agent", String(120), index=True),
    Column("action_type", String(80), index=True),
    Column("priority", String(20), index=True),
    Column("title", String(220), index=True),
    Column("description", Text),
    Column("target_type", String(80), index=True),
    Column("target_ref", String(160), index=True),
    Column("payload_json", Text),
    Column("requires_human_signoff", Boolean, default=True, index=True),
    Column("status", String(30), default="PENDING_REVIEW", index=True),
    Column("reviewer", String(120), nullable=True),
    Column("reviewer_comment", Text, nullable=True),
    Column("created_at", DateTime, default=utcnow, nullable=False, index=True),
    Column("reviewed_at", DateTime, nullable=True, index=True),
    Column("executed_at", DateTime, nullable=True, index=True),
)

FAU_CONNECTORS = Table(
    "fau_connectors",
    FAU_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("name", String(80), unique=True, index=True, nullable=False),
    Column("display_name", String(140), nullable=False),
    Column("kind", String(40), default="api", index=True),
    Column("base_url", String(255)),
    Column("db_dsn", Text),
    Column("auth_mode", String(40), default="none"),
    Column("permissions_granted", Boolean, default=False, index=True),
    Column("enabled", Boolean, default=False, index=True),
    Column("config_json", Text),
    Column("last_sync_at", DateTime, index=True),
    Column("created_at", DateTime, default=utcnow, nullable=False, index=True),
    Column("updated_at", DateTime, default=utcnow, nullable=False, index=True),
)

FAU_EXTERNAL_EVENTS = Table(
    "fau_external_events",
    FAU_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("connector_name", String(80), index=True),
    Column("external_id", String(140), index=True),
    Column("event_date", Date, index=True),
    Column("event_type", String(80), index=True),
    Column("patient_ref", String(140), index=True),
    Column("payload_json", Text),
    Column("created_at", DateTime, default=utcnow, nullable=False, index=True),
)


def _safe_text(v: Any) -> str:
    return str(v or "").strip()


def _safe_upper(v: Any) -> str:
    return _safe_text(v).upper()


def _dump(v: Any) -> str:
    try:
        return json.dumps(v, ensure_ascii=False)
    except Exception:
        return "{}"


def _load(v: Any, default: Any) -> Any:
    if v is None:
        return default
    if isinstance(v, (dict, list)):
        return v
    if isinstance(v, str):
        try:
            obj = json.loads(v)
            if isinstance(obj, type(default)):
                return obj
        except Exception:
            return default
    return default


def ensure_fau_schema(bind_or_session: Any) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    FAU_METADATA.create_all(bind=bind, checkfirst=True)


def ensure_default_connectors(db: Session) -> None:
    ensure_fau_schema(db)
    defaults = [
        {
            "name": "PHEDS",
            "display_name": "PHEDS",
            "kind": "ecosystem",
            "base_url": "",
            "auth_mode": "token",
        },
        {
            "name": "ECE",
            "display_name": "ECE",
            "kind": "ecosystem",
            "base_url": "",
            "auth_mode": "token",
        },
    ]
    for d in defaults:
        exists = db.execute(select(FAU_CONNECTORS.c.id).where(FAU_CONNECTORS.c.name == d["name"]).limit(1)).first()
        if exists:
            continue
        db.execute(
            insert(FAU_CONNECTORS).values(
                name=d["name"],
                display_name=d["display_name"],
                kind=d["kind"],
                base_url=d["base_url"],
                auth_mode=d["auth_mode"],
                permissions_granted=False,
                enabled=False,
                config_json="{}",
                created_at=utcnow(),
                updated_at=utcnow(),
            )
        )
    db.commit()


@dataclass
class AgentReport:
    agent_name: str
    scope: str
    metrics: Dict[str, Any]
    insights: List[Dict[str, Any]]
    actions: List[Dict[str, Any]] | None = None


class BaseAgent:
    name = "BASE"
    scope = "generic"

    def analyze(
        self,
        db: Session,
        sdb: Session,
        window_days: int,
        run_id: Optional[int] = None,
    ) -> AgentReport:
        return AgentReport(self.name, self.scope, {}, [])


class ConsultaAgent(BaseAgent):
    name = "AI_CONSULTA"
    scope = "consulta"

    def analyze(
        self,
        db: Session,
        sdb: Session,
        window_days: int,
        run_id: Optional[int] = None,
    ) -> AgentReport:
        from app.core.app_context import main_proxy as m

        since = date.today() - timedelta(days=window_days)
        rows = (
            db.query(m.ConsultaDB)
            .filter(m.ConsultaDB.fecha_registro >= since)
            .order_by(m.ConsultaDB.fecha_registro.asc())
            .all()
        )
        by_dx = Counter(_safe_upper(r.diagnostico_principal or "NO_REGISTRADO") for r in rows)
        by_status = Counter(_safe_upper(r.estatus_protocolo or "NO_REGISTRADO") for r in rows)
        female = sum(1 for r in rows if _safe_upper(r.sexo).startswith("F"))
        male = sum(1 for r in rows if _safe_upper(r.sexo).startswith("M"))

        insights = []
        actions: List[Dict[str, Any]] = []
        if by_status.get("COMPLETO", 0) < by_status.get("INCOMPLETO", 0):
            insights.append(
                {
                    "type": "capture_gap",
                    "severity": "MEDIA",
                    "text": "Protocolos incompletos superan a completos en la ventana analizada.",
                }
            )
            actions.append(
                {
                    "action_type": "CAPTURA_CALIDAD_CHECKLIST",
                    "priority": "MEDIA",
                    "title": "Refuerzo de captura en consulta externa",
                    "description": "Protocolos incompletos superan a completos; ejecutar checklist de campos críticos.",
                    "target_type": "consulta",
                    "target_ref": "protocolo_completo",
                    "payload": {"por_estatus_protocolo": dict(by_status)},
                    "requires_human_signoff": True,
                }
            )

        metrics = {
            "period_start": since.isoformat(),
            "period_end": date.today().isoformat(),
            "total_consultas": len(rows),
            "por_diagnostico": dict(by_dx.most_common(15)),
            "por_estatus_protocolo": dict(by_status),
            "sexo": {"MASCULINO": male, "FEMENINO": female},
        }
        return AgentReport(self.name, self.scope, metrics, insights, actions=actions)


class QuirofanoProgramadoAgent(BaseAgent):
    name = "AI_QUIROFANO_PROGRAMADO"
    scope = "quirofano_programado"

    def analyze(
        self,
        db: Session,
        sdb: Session,
        window_days: int,
        run_id: Optional[int] = None,
    ) -> AgentReport:
        from app.core.app_context import main_proxy as m

        since = date.today() - timedelta(days=window_days)
        rows = (
            sdb.query(m.SurgicalProgramacionDB)
            .filter(m.SurgicalProgramacionDB.fecha_programada >= since)
            .filter((m.SurgicalProgramacionDB.modulo_origen.is_(None)) | (m.SurgicalProgramacionDB.modulo_origen != "QUIROFANO_URGENCIA"))
            .all()
        )
        by_status = Counter(_safe_upper(r.estatus or "NO_REGISTRADO") for r in rows)
        by_proc = Counter(_safe_upper(r.procedimiento_programado or r.procedimiento or "NO_REGISTRADO") for r in rows)
        by_hgz = Counter(_safe_upper(r.hgz or "NO_REGISTRADO") for r in rows)

        wait_days: List[int] = []
        for r in rows:
            if r.fecha_programada and r.fecha_realizacion:
                wait_days.append(max((r.fecha_realizacion - r.fecha_programada).days, 0))

        metrics = {
            "period_start": since.isoformat(),
            "period_end": date.today().isoformat(),
            "total": len(rows),
            "por_estatus": dict(by_status),
            "top_procedimientos": dict(by_proc.most_common(20)),
            "top_hgz": dict(by_hgz.most_common(20)),
            "tiempo_programada_realizada_mediana_dias": statistics.median(wait_days) if wait_days else None,
            "tiempo_programada_realizada_p90_dias": _percentile(wait_days, 0.90),
            "tasa_cancelacion_pct": _pct(by_status.get("CANCELADA", 0), len(rows)),
        }

        insights = []
        actions: List[Dict[str, Any]] = []
        if metrics["tasa_cancelacion_pct"] and metrics["tasa_cancelacion_pct"] > 15:
            insights.append(
                {
                    "type": "cancel_risk",
                    "severity": "ALTA",
                    "text": "Tasa de cancelación quirúrgica programada > 15%.",
                }
            )
            actions.append(
                {
                    "action_type": "AUDIT_CANCELACION_CONCEPTOS",
                    "priority": "ALTA",
                    "title": "Auditar causas de diferimiento quirúrgico",
                    "description": "Cancelar >15% en ventana activa; priorizar auditoría por concepto, cirujano y procedimiento.",
                    "target_type": "quirofano_programado",
                    "target_ref": "cancelaciones_por_concepto",
                    "payload": {"tasa_cancelacion_pct": metrics["tasa_cancelacion_pct"]},
                    "requires_human_signoff": True,
                }
            )
        return AgentReport(self.name, self.scope, metrics, insights, actions=actions)


class QuirofanoUrgenciasAgent(BaseAgent):
    name = "AI_QUIROFANO_URGENCIAS"
    scope = "quirofano_urgencias"

    def analyze(
        self,
        db: Session,
        sdb: Session,
        window_days: int,
        run_id: Optional[int] = None,
    ) -> AgentReport:
        from app.core.app_context import main_proxy as m

        since = date.today() - timedelta(days=window_days)
        rows = (
            sdb.query(m.SurgicalUrgenciaProgramacionDB)
            .filter(m.SurgicalUrgenciaProgramacionDB.fecha_urgencia >= since)
            .all()
        )

        by_status = Counter(_safe_upper(r.estatus or "NO_REGISTRADO") for r in rows)
        by_dx = Counter(_safe_upper(r.patologia or "NO_REGISTRADO") for r in rows)
        by_proc = Counter(_safe_upper(r.procedimiento_programado or "NO_REGISTRADO") for r in rows)

        hemoderivados = sum(
            int(r.hemoderivados_pg_solicitados or 0) + int(r.hemoderivados_pfc_solicitados or 0) + int(r.hemoderivados_cp_solicitados or 0)
            for r in rows
            if _safe_upper(r.solicita_hemoderivados) == "SI"
        )

        metrics = {
            "period_start": since.isoformat(),
            "period_end": date.today().isoformat(),
            "total": len(rows),
            "por_estatus": dict(by_status),
            "top_diagnosticos": dict(by_dx.most_common(20)),
            "top_procedimientos": dict(by_proc.most_common(20)),
            "hemoderivados_solicitados_total_unidades": hemoderivados,
            "tasa_realizacion_pct": _pct(by_status.get("REALIZADA", 0), len(rows)),
        }
        insights = []
        actions: List[Dict[str, Any]] = []
        if hemoderivados > 0:
            insights.append(
                {
                    "type": "resource_alert",
                    "severity": "MEDIA",
                    "text": f"Se detectaron {hemoderivados} unidades de hemoderivados solicitadas en urgencias.",
                }
            )
            actions.append(
                {
                    "action_type": "HEMODERIVADOS_STOCK_REVIEW",
                    "priority": "MEDIA",
                    "title": "Revisión de hemoderivados en urgencias",
                    "description": "Se detectó demanda de hemoderivados; verificar disponibilidad y tendencia semanal.",
                    "target_type": "urgencias",
                    "target_ref": "hemoderivados",
                    "payload": {"unidades_total": hemoderivados},
                    "requires_human_signoff": True,
                }
            )
        return AgentReport(self.name, self.scope, metrics, insights, actions=actions)


class QuirofanoAgent(BaseAgent):
    name = "AI_QUIROFANO_AGENT"
    scope = "quirofano_predictivo"

    def analyze(
        self,
        db: Session,
        sdb: Session,
        window_days: int,
        run_id: Optional[int] = None,
    ) -> AgentReport:
        from app.core.app_context import main_proxy as m

        result = analyze_quirofano_programaciones(
            db,
            sdb,
            m,
            window_days=window_days,
            run_id=run_id,
            limit=700,
        )
        return AgentReport(self.name, self.scope, result.get("metrics", {}), result.get("insights", []))


class HospitalizacionAgent(BaseAgent):
    name = "AI_HOSPITALIZACION"
    scope = "hospitalizacion"

    def analyze(
        self,
        db: Session,
        sdb: Session,
        window_days: int,
        run_id: Optional[int] = None,
    ) -> AgentReport:
        from app.core.app_context import main_proxy as m

        result = analyze_hospitalizacion_patients(
            db,
            sdb,
            m,
            window_days=window_days,
            run_id=run_id,
            model_version="hospitalizacion_agent_v1",
        )
        return AgentReport(self.name, self.scope, result.get("metrics", {}), result.get("insights", []))


class LaboratorioAgent(BaseAgent):
    name = "AI_LABORATORIOS"
    scope = "laboratorios"

    def analyze(
        self,
        db: Session,
        sdb: Session,
        window_days: int,
        run_id: Optional[int] = None,
    ) -> AgentReport:
        from app.core.app_context import main_proxy as m

        since_dt = utcnow() - timedelta(days=window_days)
        rows = (
            db.query(m.LabDB)
            .filter(m.LabDB.timestamp >= since_dt)
            .order_by(m.LabDB.timestamp.asc())
            .all()
        )

        metrics_bucket = {
            "creatinina": [],
            "hemoglobina": [],
            "leucocitos": [],
            "plaquetas": [],
            "sodio": [],
            "potasio": [],
        }

        for r in rows:
            marker = _map_lab_marker(_safe_text(r.test_name), _safe_text(r.test_code))
            if not marker:
                continue
            value = _extract_float(r.value)
            if value is None:
                continue
            metrics_bucket[marker].append(value)

        incidencias = {
            "aki_cr_ge_2": sum(1 for x in metrics_bucket["creatinina"] if x >= 2.0),
            "hb_lt_8": sum(1 for x in metrics_bucket["hemoglobina"] if x < 8.0),
            "leuco_gt_10000": sum(1 for x in metrics_bucket["leucocitos"] if x > 10000),
            "plt_lt_150": sum(1 for x in metrics_bucket["plaquetas"] if x < 150),
            "disnatremia": sum(1 for x in metrics_bucket["sodio"] if x < 135 or x > 145),
            "dispotasemia": sum(1 for x in metrics_bucket["potasio"] if x < 3.5 or x > 5.0),
        }

        metrics = {
            "period_start": since_dt.date().isoformat(),
            "period_end": date.today().isoformat(),
            "total_registros_laboratorio": len(rows),
            "analitos_detectados": {k: len(v) for k, v in metrics_bucket.items()},
            "incidencias": incidencias,
        }

        insights = []
        actions: List[Dict[str, Any]] = []
        if incidencias["aki_cr_ge_2"] > 0:
            insights.append(
                {
                    "type": "renal_alert",
                    "severity": "ALTA",
                    "text": f"Se detectaron {incidencias['aki_cr_ge_2']} determinaciones con creatinina >= 2.0.",
                }
            )
            actions.append(
                {
                    "action_type": "AKI_SURVEILLANCE_LIST",
                    "priority": "ALTA",
                    "title": "Activar cohorte AKI",
                    "description": "Creatinina elevada detectada; revisar pacientes activos y estancias prolongadas asociadas.",
                    "target_type": "laboratorios",
                    "target_ref": "aki_cr_ge_2",
                    "payload": {"incidencias": incidencias},
                    "requires_human_signoff": True,
                }
            )
        return AgentReport(self.name, self.scope, metrics, insights, actions=actions)


class ReporteEstadisticoAgent(BaseAgent):
    name = "AI_REPORTE_ESTADISTICO"
    scope = "reporte"

    def analyze(
        self,
        db: Session,
        sdb: Session,
        window_days: int,
        run_id: Optional[int] = None,
    ) -> AgentReport:
        from app.core.app_context import main_proxy as m

        since_dt = utcnow() - timedelta(days=window_days)
        rows = (
            sdb.query(m.HechoFlujoQuirurgico)
            .filter(m.HechoFlujoQuirurgico.creado_en >= since_dt)
            .all()
        )
        by_event = Counter(_safe_upper(r.evento or "NO_REGISTRADO") for r in rows)
        by_status = Counter(_safe_upper(r.estatus or "NO_REGISTRADO") for r in rows)

        embudo = {
            "ingreso": by_event.get("INGRESO", 0),
            "programacion": by_event.get("PROGRAMADA", 0) + by_event.get("URG_PROGRAMADA", 0),
            "cirugia_realizada": by_event.get("REALIZADA", 0) + by_event.get("URG_REALIZADA", 0),
            "postquirurgica": by_event.get("POSTQUIRURGICA", 0),
            "alta": by_event.get("ALTA", 0),
        }

        metrics = {
            "period_start": since_dt.date().isoformat(),
            "period_end": date.today().isoformat(),
            "total_eventos": len(rows),
            "por_evento": dict(by_event),
            "por_estatus": dict(by_status),
            "embudo_operativo": embudo,
            "conversiones": {
                "programacion_a_realizada_pct": _pct(embudo["cirugia_realizada"], embudo["programacion"]),
                "realizada_a_postqx_pct": _pct(embudo["postquirurgica"], embudo["cirugia_realizada"]),
                "postqx_a_alta_pct": _pct(embudo["alta"], embudo["postquirurgica"]),
            },
        }
        insights = []
        actions: List[Dict[str, Any]] = []
        if metrics["conversiones"]["programacion_a_realizada_pct"] < 60:
            insights.append(
                {
                    "type": "funnel_drop",
                    "severity": "MEDIA",
                    "text": "Conversión programación→realizada por debajo de 60%.",
                }
            )
            actions.append(
                {
                    "action_type": "FUNNEL_REVIEW",
                    "priority": "MEDIA",
                    "title": "Revisar embudo operativo quirúrgico",
                    "description": "Conversión baja en embudo; revisar cuellos de botella entre programación y cirugía realizada.",
                    "target_type": "reporte",
                    "target_ref": "embudo_operativo",
                    "payload": {"conversiones": metrics.get("conversiones", {})},
                    "requires_human_signoff": True,
                }
            )

        return AgentReport(self.name, self.scope, metrics, insights, actions=actions)


def _map_lab_marker(name: str, code: str) -> Optional[str]:
    joined = f"{name} {code}".lower()
    if "creatin" in joined or joined.strip() == "cr":
        return "creatinina"
    if "hemoglob" in joined or "hgb" in joined or joined.strip().startswith("hb"):
        return "hemoglobina"
    if "leuco" in joined or "wbc" in joined:
        return "leucocitos"
    if "plaquet" in joined or "plt" in joined:
        return "plaquetas"
    if "sodio" in joined or joined.strip() == "na":
        return "sodio"
    if "potasio" in joined or joined.strip() == "k":
        return "potasio"
    return None


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


def _pct(part: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round((float(part) / float(total)) * 100.0, 2)


def _percentile(values: List[int], p: float) -> Optional[float]:
    if not values:
        return None
    if len(values) == 1:
        return float(values[0])
    sorted_vals = sorted(values)
    idx = int(math.ceil(p * len(sorted_vals))) - 1
    idx = max(0, min(idx, len(sorted_vals) - 1))
    return float(sorted_vals[idx])


def _build_daily_series(report_metrics: Dict[str, Dict[str, Any]], days: int) -> Dict[str, List[Dict[str, Any]]]:
    # serie sintética mínima a partir de métricas globales para permitir tendencia base
    series: Dict[str, List[Dict[str, Any]]] = {}
    horizon = max(7, min(days, 90))
    for agent, metrics in report_metrics.items():
        total = int(metrics.get("total") or metrics.get("total_consultas") or metrics.get("total_ingresos") or metrics.get("total_eventos") or 0)
        avg = total / float(horizon) if horizon > 0 else 0.0
        points: List[Dict[str, Any]] = []
        for i in range(horizon):
            day = (date.today() - timedelta(days=horizon - 1 - i)).isoformat()
            points.append({"date": day, "value": round(avg, 2)})
        series[agent] = points
    return series


def _simple_projection(series: List[float], horizon: int = 7) -> List[float]:
    if not series:
        return [0.0] * horizon
    if len(series) == 1:
        return [round(series[0], 2)] * horizon

    n = len(series)
    x = list(range(n))
    y = series
    x_mean = sum(x) / n
    y_mean = sum(y) / n
    num = sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, y))
    den = sum((xi - x_mean) ** 2 for xi in x) or 1.0
    slope = num / den
    intercept = y_mean - slope * x_mean

    out: List[float] = []
    start = n
    for i in range(horizon):
        pred = intercept + slope * (start + i)
        out.append(round(max(pred, 0.0), 2))
    return out


def _insert_report(db: Session, run_id: int, rep: AgentReport) -> None:
    db.execute(
        insert(FAU_AGENT_REPORTS).values(
            run_id=run_id,
            agent_name=rep.agent_name,
            scope=rep.scope,
            report_date=date.today(),
            metrics_json=_dump(rep.metrics),
            insights_json=_dump(rep.insights),
            created_at=utcnow(),
        )
    )


def _insert_message(db: Session, run_id: int, from_agent: str, to_agent: str, message_type: str, severity: str, payload: Dict[str, Any]) -> None:
    db.execute(
        insert(FAU_AGENT_MESSAGES).values(
            run_id=run_id,
            from_agent=from_agent,
            to_agent=to_agent,
            message_type=message_type,
            severity=severity,
            payload_json=_dump(payload),
            created_at=utcnow(),
        )
    )


def _insert_alert(db: Session, run_id: int, title: str, severity: str, category: str, description: str, recommendation: str, payload: Dict[str, Any]) -> None:
    db.execute(
        insert(FAU_CENTRAL_ALERTS).values(
            run_id=run_id,
            title=title,
            severity=severity,
            category=category,
            description=description,
            recommendation=recommendation,
            payload_json=_dump(payload),
            created_at=utcnow(),
        )
    )


def _insert_action(
    db: Session,
    run_id: int,
    *,
    source_agent: str,
    action_type: str,
    priority: str,
    title: str,
    description: str,
    target_type: str = "",
    target_ref: str = "",
    payload: Optional[Dict[str, Any]] = None,
    requires_human_signoff: bool = True,
) -> None:
    db.execute(
        insert(FAU_ACTION_PROPOSALS).values(
            run_id=run_id,
            source_agent=_safe_text(source_agent) or "FAU_BOT",
            action_type=_safe_text(action_type) or "ACTION",
            priority=_safe_upper(priority) or "MEDIA",
            title=_safe_text(title)[:220],
            description=_safe_text(description),
            target_type=_safe_text(target_type) or None,
            target_ref=_safe_text(target_ref) or None,
            payload_json=_dump(payload or {}),
            requires_human_signoff=bool(requires_human_signoff),
            status="PENDING_REVIEW",
            created_at=utcnow(),
        )
    )


def _emit_inter_agent_messages(db: Session, run_id: int, report_metrics: Dict[str, Dict[str, Any]]) -> int:
    emitted = 0
    consulta = report_metrics.get("AI_CONSULTA", {})
    hospitalizacion = report_metrics.get("AI_HOSPITALIZACION", {})
    laboratorios = report_metrics.get("AI_LABORATORIOS", {})
    qx_programado = report_metrics.get("AI_QUIROFANO_PROGRAMADO", {})
    qx_urgencias = report_metrics.get("AI_QUIROFANO_URGENCIAS", {})
    qx_predictivo = report_metrics.get("AI_QUIROFANO_AGENT", {})

    protocolos = consulta.get("por_estatus_protocolo") or {}
    incompleto = int(protocolos.get("INCOMPLETO", 0))
    completo = int(protocolos.get("COMPLETO", 0))
    if incompleto > completo:
        _insert_message(
            db,
            run_id,
            from_agent="AI_CONSULTA",
            to_agent="AI_REPORTE_ESTADISTICO",
            message_type="DATA_QUALITY_SIGNAL",
            severity="MEDIA",
            payload={"incompleto": incompleto, "completo": completo},
        )
        emitted += 1

    prolongada = float(hospitalizacion.get("indice_estancia_prolongada_pct") or 0.0)
    if prolongada >= 25:
        _insert_message(
            db,
            run_id,
            from_agent="AI_HOSPITALIZACION",
            to_agent="AI_QUIROFANO_PROGRAMADO",
            message_type="FLOW_FEEDBACK",
            severity="MEDIA",
            payload={"indice_estancia_prolongada_pct": prolongada},
        )
        emitted += 1

    lab_aki = int((laboratorios.get("incidencias") or {}).get("aki_cr_ge_2") or 0)
    if lab_aki > 0:
        _insert_message(
            db,
            run_id,
            from_agent="AI_LABORATORIOS",
            to_agent="AI_HOSPITALIZACION",
            message_type="CLINICAL_SIGNAL",
            severity="ALTA",
            payload={"aki_cr_ge_2": lab_aki},
        )
        emitted += 1

    cancel_pct = float(qx_programado.get("tasa_cancelacion_pct") or 0.0)
    if cancel_pct >= 15:
        _insert_message(
            db,
            run_id,
            from_agent="AI_QUIROFANO_PROGRAMADO",
            to_agent="AI_REPORTE_ESTADISTICO",
            message_type="OPERATIVE_SIGNAL",
            severity="MEDIA",
            payload={"tasa_cancelacion_pct": cancel_pct},
        )
        emitted += 1

    hemoderivados = int(qx_urgencias.get("hemoderivados_solicitados_total_unidades") or 0)
    if hemoderivados > 0:
        _insert_message(
            db,
            run_id,
            from_agent="AI_QUIROFANO_URGENCIAS",
            to_agent="AI_HOSPITALIZACION",
            message_type="RESOURCE_SIGNAL",
            severity="MEDIA",
            payload={"hemoderivados_unidades": hemoderivados},
        )
        emitted += 1

    qx_high_cancel = int(qx_predictivo.get("alto_riesgo_cancelacion") or 0)
    qx_high_comp = int(qx_predictivo.get("alto_riesgo_complicacion") or 0)
    qx_total = int(qx_predictivo.get("total_programaciones_analizadas") or 0)
    if qx_total > 0 and qx_high_comp > 0:
        _insert_message(
            db,
            run_id,
            from_agent="AI_QUIROFANO_AGENT",
            to_agent="AI_HOSPITALIZACION",
            message_type="POSTQX_RISK_SIGNAL",
            severity="ALTA" if (qx_high_comp / float(qx_total)) >= 0.2 else "MEDIA",
            payload={
                "alto_riesgo_complicacion": qx_high_comp,
                "total_programaciones_analizadas": qx_total,
            },
        )
        emitted += 1

    if qx_total > 0 and qx_high_cancel > 0:
        _insert_message(
            db,
            run_id,
            from_agent="AI_QUIROFANO_AGENT",
            to_agent="AI_REPORTE_ESTADISTICO",
            message_type="CANCELATION_RISK_SIGNAL",
            severity="MEDIA",
            payload={
                "alto_riesgo_cancelacion": qx_high_cancel,
                "total_programaciones_analizadas": qx_total,
            },
        )
        emitted += 1

    return emitted


def run_fau_bot_cycle(
    db: Session,
    sdb: Session,
    *,
    window_days: int = 30,
    triggered_by: str = "manual",
) -> Dict[str, Any]:
    ensure_default_connectors(db)
    ensure_hospital_agent_schema(sdb)
    ensure_quirofano_agent_schema(sdb)

    run_res = db.execute(
        insert(FAU_AGENT_RUNS).values(
            triggered_by=_safe_text(triggered_by) or "manual",
            window_days=max(1, min(window_days, 365)),
            status="RUNNING",
            started_at=utcnow(),
        )
    )
    run_id = int(run_res.inserted_primary_key[0])

    agents: List[BaseAgent] = [
        ConsultaAgent(),
        QuirofanoProgramadoAgent(),
        QuirofanoAgent(),
        QuirofanoUrgenciasAgent(),
        HospitalizacionAgent(),
        LaboratorioAgent(),
        ReporteEstadisticoAgent(),
    ]

    report_metrics: Dict[str, Dict[str, Any]] = {}
    alert_count = 0
    message_count = 0
    action_count = 0

    for agent in agents:
        rep = agent.analyze(db, sdb, window_days, run_id=run_id)
        report_metrics[agent.name] = rep.metrics
        _insert_report(db, run_id, rep)

        # Compartir con FAU_BOT central
        _insert_message(
            db,
            run_id,
            from_agent=agent.name,
            to_agent="FAU_BOT",
            message_type="AGENT_REPORT",
            severity="INFO",
            payload={"metrics": rep.metrics, "insights": rep.insights, "actions": rep.actions or []},
        )
        message_count += 1

        for ins in rep.insights:
            severity = _safe_upper(ins.get("severity") or "MEDIA")
            _insert_alert(
                db,
                run_id,
                title=f"{agent.name}::{ins.get('type', 'insight')}",
                severity=severity,
                category=rep.scope,
                description=_safe_text(ins.get("text")),
                recommendation="Revisar panel del módulo y ajustar estrategia clínica-operativa.",
                payload={"agent": agent.name, "insight": ins},
            )
            alert_count += 1
            if severity in {"ALTA", "CRITICAL"}:
                _insert_action(
                    db,
                    run_id,
                    source_agent=agent.name,
                    action_type="REVIEW_INSIGHT",
                    priority="ALTA",
                    title=f"{agent.name}: revisar alerta {ins.get('type', 'insight')}",
                    description=_safe_text(ins.get("text")),
                    target_type=rep.scope,
                    target_ref=agent.name,
                    payload={"insight": ins, "metrics": rep.metrics},
                    requires_human_signoff=True,
                )
                action_count += 1

        for proposal in rep.actions or []:
            try:
                _insert_action(
                    db,
                    run_id,
                    source_agent=agent.name,
                    action_type=_safe_text(proposal.get("action_type")) or "ACTION",
                    priority=_safe_upper(proposal.get("priority") or "MEDIA"),
                    title=_safe_text(proposal.get("title")) or f"{agent.name} acción propuesta",
                    description=_safe_text(proposal.get("description")),
                    target_type=_safe_text(proposal.get("target_type")) or rep.scope,
                    target_ref=_safe_text(proposal.get("target_ref")) or agent.name,
                    payload=proposal.get("payload") if isinstance(proposal.get("payload"), dict) else {"proposal": proposal},
                    requires_human_signoff=bool(proposal.get("requires_human_signoff", True)),
                )
                action_count += 1
            except Exception:
                # No bloquear ciclo central por una propuesta de acción malformada.
                continue

    message_count += _emit_inter_agent_messages(db, run_id, report_metrics)

    # Síntesis central (fusión inter-agente)
    hosp = report_metrics.get("AI_HOSPITALIZACION", {})
    labs = report_metrics.get("AI_LABORATORIOS", {})
    qx_prog = report_metrics.get("AI_QUIROFANO_PROGRAMADO", {})
    qx_pred = report_metrics.get("AI_QUIROFANO_AGENT", {})

    est_prolong = float(hosp.get("indice_estancia_prolongada_pct") or 0.0)
    aki_count = int((labs.get("incidencias") or {}).get("aki_cr_ge_2") or 0)
    cancel_pct = float(qx_prog.get("tasa_cancelacion_pct") or 0.0)

    if est_prolong > 20 and aki_count > 0:
        _insert_alert(
            db,
            run_id,
            title="FAU_BOT::Riesgo de complicación en estancia",
            severity="ALTA",
            category="fusion_clinica",
            description="Coinciden estancia prolongada elevada con eventos de creatinina alta en la ventana analizada.",
            recommendation="Priorizar cohorte de riesgo renal, reforzar vigilancia de laboratorio y revisión por guardia.",
            payload={"estancia_prolongada_pct": est_prolong, "aki_count": aki_count},
        )
        alert_count += 1
        _insert_message(
            db,
            run_id,
            from_agent="FAU_BOT",
            to_agent="AI_HOSPITALIZACION",
            message_type="FOCUSED_ACTION",
            severity="ALTA",
            payload={"action": "priorizar_seguimiento_renal", "motivo": "estancia_prolongada+aki"},
        )
        message_count += 1
        _insert_action(
            db,
            run_id,
            source_agent="FAU_BOT",
            action_type="RENAL_SURVEILLANCE_COHORT",
            priority="ALTA",
            title="Priorizar cohorte renal hospitalización",
            description="Estancia prolongada + creatinina elevada detectadas. Revisar cohorte y ejecutar vigilancia intensiva.",
            target_type="hospitalizacion",
            target_ref="cohorte_renal",
            payload={"estancia_prolongada_pct": est_prolong, "aki_count": aki_count},
            requires_human_signoff=True,
        )
        action_count += 1

    if cancel_pct > 15:
        _insert_message(
            db,
            run_id,
            from_agent="FAU_BOT",
            to_agent="AI_QUIROFANO_PROGRAMADO",
            message_type="FOCUSED_ACTION",
            severity="MEDIA",
            payload={"action": "auditar_cancelaciones", "tasa_cancelacion_pct": cancel_pct},
        )
        message_count += 1
        _insert_action(
            db,
            run_id,
            source_agent="FAU_BOT",
            action_type="AUDIT_CANCELACIONES",
            priority="MEDIA",
            title="Auditar cancelaciones quirúrgicas",
            description="Tasa de cancelación programada superior a umbral operativo.",
            target_type="quirofano_programado",
            target_ref="cancelaciones",
            payload={"tasa_cancelacion_pct": cancel_pct},
            requires_human_signoff=True,
        )
        action_count += 1

    qx_high_comp = int(qx_pred.get("alto_riesgo_complicacion") or 0)
    qx_total = int(qx_pred.get("total_programaciones_analizadas") or 0)
    if qx_total > 0 and (qx_high_comp / float(qx_total)) >= 0.2:
        _insert_alert(
            db,
            run_id,
            title="FAU_BOT::Riesgo postquirúrgico elevado",
            severity="ALTA",
            category="fusion_quirofano",
            description="AI_QUIROFANO_AGENT detecta proporción alta de riesgo de complicación quirúrgica.",
            recommendation="Revisar alertas de quirófano por programación e intensificar vigilancia postquirúrgica.",
            payload={
                "alto_riesgo_complicacion": qx_high_comp,
                "total_programaciones_analizadas": qx_total,
            },
        )
        alert_count += 1
        _insert_action(
            db,
            run_id,
            source_agent="FAU_BOT",
            action_type="POSTQX_HIGH_RISK_ROUND",
            priority="ALTA",
            title="Ronda clínica postquirúrgica de alto riesgo",
            description="Proporción elevada de riesgo de complicación quirúrgica en la ventana analizada.",
            target_type="quirofano",
            target_ref="postqx_alto_riesgo",
            payload={
                "alto_riesgo_complicacion": qx_high_comp,
                "total_programaciones_analizadas": qx_total,
            },
            requires_human_signoff=True,
        )
        action_count += 1

    daily_series = _build_daily_series(report_metrics, window_days)
    projections: Dict[str, Any] = {}
    for agent_name, points in daily_series.items():
        vals = [float(p["value"]) for p in points]
        projections[agent_name] = {
            "horizon_days": 7,
            "predicted": _simple_projection(vals, horizon=7),
        }

    central_summary = {
        "run_id": run_id,
        "generated_at": utcnow().isoformat(),
        "window_days": window_days,
        "module_reports": report_metrics,
        "predictions": projections,
        "alert_count": alert_count,
        "message_count": message_count,
        "action_count": action_count,
    }

    _insert_report(
        db,
        run_id,
        AgentReport(
            agent_name="FAU_BOT",
            scope="central",
            metrics={
                "window_days": window_days,
                "alert_count": alert_count,
                "message_count": message_count,
                "action_count": action_count,
                "modules": list(report_metrics.keys()),
            },
            insights=[
                {
                    "type": "predictive_summary",
                    "severity": "INFO",
                    "text": "Síntesis central y proyección base generadas para todos los módulos.",
                }
            ],
        ),
    )

    db.execute(
        update(FAU_AGENT_RUNS)
        .where(FAU_AGENT_RUNS.c.id == run_id)
        .values(
            status="COMPLETED",
            ended_at=utcnow(),
            summary_json=_dump(central_summary),
        )
    )
    db.commit()

    return central_summary


def list_action_proposals(
    db: Session,
    *,
    status: Optional[str] = None,
    limit: int = 200,
) -> List[Dict[str, Any]]:
    ensure_fau_schema(db)
    q = select(FAU_ACTION_PROPOSALS)
    if status:
        q = q.where(FAU_ACTION_PROPOSALS.c.status == _safe_upper(status))
    rows = db.execute(q.order_by(desc(FAU_ACTION_PROPOSALS.c.id)).limit(max(1, min(limit, 1000)))).mappings().all()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "run_id": r["run_id"],
                "source_agent": r["source_agent"],
                "action_type": r["action_type"],
                "priority": r["priority"],
                "title": r["title"],
                "description": r["description"],
                "target_type": r["target_type"],
                "target_ref": r["target_ref"],
                "payload": _load(r["payload_json"], {}),
                "requires_human_signoff": bool(r["requires_human_signoff"]),
                "status": r["status"],
                "reviewer": r["reviewer"],
                "reviewer_comment": r["reviewer_comment"],
                "created_at": r["created_at"].isoformat() if r["created_at"] else None,
                "reviewed_at": r["reviewed_at"].isoformat() if r["reviewed_at"] else None,
                "executed_at": r["executed_at"].isoformat() if r["executed_at"] else None,
            }
        )
    return out


def set_action_proposal_status(
    db: Session,
    *,
    action_id: int,
    status: str,
    reviewer: str = "",
    reviewer_comment: str = "",
) -> Dict[str, Any]:
    ensure_fau_schema(db)
    aid = int(action_id or 0)
    if aid <= 0:
        raise ValueError("action_id inválido")
    normalized = _safe_upper(status)
    allowed = {"PENDING_REVIEW", "APPROVED", "REJECTED", "EXECUTED"}
    if normalized not in allowed:
        raise ValueError(f"status inválido: {normalized}")

    row = db.execute(select(FAU_ACTION_PROPOSALS).where(FAU_ACTION_PROPOSALS.c.id == aid).limit(1)).mappings().first()
    if not row:
        raise ValueError("Action proposal no encontrada")

    values: Dict[str, Any] = {
        "status": normalized,
        "reviewer": _safe_text(reviewer) or row.get("reviewer"),
        "reviewer_comment": _safe_text(reviewer_comment) or row.get("reviewer_comment"),
    }
    if normalized in {"APPROVED", "REJECTED", "EXECUTED"}:
        values["reviewed_at"] = utcnow()
    if normalized == "EXECUTED":
        values["executed_at"] = utcnow()

    db.execute(update(FAU_ACTION_PROPOSALS).where(FAU_ACTION_PROPOSALS.c.id == aid).values(**values))
    db.commit()

    latest = list_action_proposals(db, limit=1, status="")
    for item in latest:
        if int(item["id"]) == aid:
            return item
    raise ValueError("No se pudo recuperar action proposal actualizada")


def list_runs(db: Session, limit: int = 30) -> List[Dict[str, Any]]:
    ensure_fau_schema(db)
    rows = db.execute(
        select(FAU_AGENT_RUNS).order_by(desc(FAU_AGENT_RUNS.c.id)).limit(max(1, min(limit, 300)))
    ).mappings().all()
    out = []
    for r in rows:
        out.append(
            {
                "id": r["id"],
                "triggered_by": r["triggered_by"],
                "window_days": r["window_days"],
                "status": r["status"],
                "started_at": r["started_at"].isoformat() if r["started_at"] else None,
                "ended_at": r["ended_at"].isoformat() if r["ended_at"] else None,
                "summary": _load(r["summary_json"], {}),
            }
        )
    return out


def latest_reports(db: Session, agent_name: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
    ensure_fau_schema(db)
    q = select(FAU_AGENT_REPORTS)
    if agent_name:
        q = q.where(FAU_AGENT_REPORTS.c.agent_name == _safe_text(agent_name).upper())
    rows = db.execute(q.order_by(desc(FAU_AGENT_REPORTS.c.id)).limit(max(1, min(limit, 300)))).mappings().all()
    return [
        {
            "id": r["id"],
            "run_id": r["run_id"],
            "agent_name": r["agent_name"],
            "scope": r["scope"],
            "report_date": r["report_date"].isoformat() if r["report_date"] else None,
            "metrics": _load(r["metrics_json"], {}),
            "insights": _load(r["insights_json"], []),
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
        for r in rows
    ]


def latest_alerts(db: Session, limit: int = 50) -> List[Dict[str, Any]]:
    ensure_fau_schema(db)
    rows = db.execute(
        select(FAU_CENTRAL_ALERTS).order_by(desc(FAU_CENTRAL_ALERTS.c.id)).limit(max(1, min(limit, 500)))
    ).mappings().all()
    return [
        {
            "id": r["id"],
            "run_id": r["run_id"],
            "title": r["title"],
            "severity": r["severity"],
            "category": r["category"],
            "description": r["description"],
            "recommendation": r["recommendation"],
            "payload": _load(r["payload_json"], {}),
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
        for r in rows
    ]


def list_connectors(db: Session) -> List[Dict[str, Any]]:
    ensure_default_connectors(db)
    rows = db.execute(select(FAU_CONNECTORS).order_by(FAU_CONNECTORS.c.name.asc())).mappings().all()
    out = []
    for r in rows:
        out.append(
            {
                "name": r["name"],
                "display_name": r["display_name"],
                "kind": r["kind"],
                "base_url": r["base_url"],
                "auth_mode": r["auth_mode"],
                "permissions_granted": bool(r["permissions_granted"]),
                "enabled": bool(r["enabled"]),
                "config": _load(r["config_json"], {}),
                "last_sync_at": r["last_sync_at"].isoformat() if r["last_sync_at"] else None,
            }
        )
    return out


def upsert_connector(
    db: Session,
    *,
    name: str,
    display_name: Optional[str] = None,
    kind: str = "api",
    base_url: str = "",
    db_dsn: str = "",
    auth_mode: str = "none",
    enabled: Optional[bool] = None,
    permissions_granted: Optional[bool] = None,
    config: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    ensure_default_connectors(db)

    key = _safe_upper(name)
    row = db.execute(select(FAU_CONNECTORS).where(FAU_CONNECTORS.c.name == key).limit(1)).mappings().first()

    values = {
        "display_name": _safe_text(display_name or key),
        "kind": _safe_text(kind or "api"),
        "base_url": _safe_text(base_url),
        "db_dsn": _safe_text(db_dsn),
        "auth_mode": _safe_text(auth_mode or "none"),
        "updated_at": utcnow(),
    }
    if enabled is not None:
        values["enabled"] = bool(enabled)
    if permissions_granted is not None:
        values["permissions_granted"] = bool(permissions_granted)
    if config is not None:
        values["config_json"] = _dump(config)

    if row:
        db.execute(update(FAU_CONNECTORS).where(FAU_CONNECTORS.c.name == key).values(**values))
    else:
        values.update(
            {
                "name": key,
                "permissions_granted": bool(permissions_granted) if permissions_granted is not None else False,
                "enabled": bool(enabled) if enabled is not None else False,
                "config_json": _dump(config or {}),
                "created_at": utcnow(),
            }
        )
        db.execute(insert(FAU_CONNECTORS).values(**values))

    db.commit()
    return [c for c in list_connectors(db) if c["name"] == key][0]


def ingest_external_event(
    db: Session,
    *,
    connector_name: str,
    event_type: str,
    payload: Dict[str, Any],
    event_date: Optional[date] = None,
    external_id: str = "",
    patient_ref: str = "",
) -> Dict[str, Any]:
    ensure_default_connectors(db)
    key = _safe_upper(connector_name)
    conn = db.execute(select(FAU_CONNECTORS).where(FAU_CONNECTORS.c.name == key).limit(1)).mappings().first()
    if not conn:
        raise ValueError("Conector no existe")
    if not bool(conn["permissions_granted"]):
        raise PermissionError("Permisos no otorgados para este conector")

    if event_date is None:
        event_date = date.today()

    db.execute(
        insert(FAU_EXTERNAL_EVENTS).values(
            connector_name=key,
            external_id=_safe_text(external_id),
            event_date=event_date,
            event_type=_safe_text(event_type) or "EXTERNAL_EVENT",
            patient_ref=_safe_text(patient_ref),
            payload_json=_dump(payload),
            created_at=utcnow(),
        )
    )
    db.execute(
        update(FAU_CONNECTORS)
        .where(FAU_CONNECTORS.c.name == key)
        .values(last_sync_at=utcnow(), updated_at=utcnow())
    )
    db.commit()
    return {"status": "ok", "connector": key}


def latest_external_events(db: Session, connector_name: Optional[str] = None, limit: int = 100) -> List[Dict[str, Any]]:
    ensure_fau_schema(db)
    q = select(FAU_EXTERNAL_EVENTS)
    if connector_name:
        q = q.where(FAU_EXTERNAL_EVENTS.c.connector_name == _safe_upper(connector_name))
    rows = db.execute(q.order_by(desc(FAU_EXTERNAL_EVENTS.c.id)).limit(max(1, min(limit, 500)))).mappings().all()
    return [
        {
            "id": r["id"],
            "connector_name": r["connector_name"],
            "external_id": r["external_id"],
            "event_date": r["event_date"].isoformat() if r["event_date"] else None,
            "event_type": r["event_type"],
            "patient_ref": r["patient_ref"],
            "payload": _load(r["payload_json"], {}),
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
        for r in rows
    ]


def get_dashboard_payload(db: Session) -> Dict[str, Any]:
    runs = list_runs(db, limit=10)
    reports = latest_reports(db, limit=20)
    alerts = latest_alerts(db, limit=20)
    actions = list_action_proposals(db, limit=20)
    connectors = list_connectors(db)
    return {
        "runs": runs,
        "reports": reports,
        "alerts": alerts,
        "actions": actions,
        "connectors": connectors,
    }
