from __future__ import annotations

import json
import os
import re
from collections import Counter
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional

from app.core.time_utils import utcnow

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
    insert,
    select,
)
from sqlalchemy.orm import Session

from app.ai_agents.model_registry import load_model_cached

try:
    import joblib
except Exception:
    joblib = None


try:
    import pandas as pd
except Exception:
    pd = None


QUIROFANO_AGENT_METADATA = MetaData()
JSON_SQL = Text().with_variant(Text(), "sqlite")

ALERTAS = Table(
    "alertas",
    QUIROFANO_AGENT_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("tipo", String(50), index=True),  # QUIROFANO / IA_CENTRAL / etc.
    Column("nivel", String(20), index=True),  # INFO / WARNING / CRITICAL
    Column("mensaje", Text),
    Column("programacion_id", Integer, nullable=True, index=True),
    Column("detalles", JSON_SQL),
    Column("fecha_creacion", DateTime, default=utcnow, index=True),
    Column("leida", Boolean, default=False, index=True),
    Column("resuelta", Boolean, default=False, index=True),
    Column("usuario_asignado", String(100), nullable=True),
)

QUIROFANO_PREDICCIONES = Table(
    "quirofano_predicciones",
    QUIROFANO_AGENT_METADATA,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("run_id", Integer, index=True),
    Column("programacion_id", Integer, index=True, nullable=False),
    Column("consulta_id", Integer, index=True),
    Column("nss", String(20), index=True),
    Column("paciente", String(220), index=True),
    Column("prediction_date", Date, index=True, nullable=False),
    Column("riesgo_cancelacion", Float, index=True),
    Column("duracion_estimada_min", Float, index=True),
    Column("riesgo_complicacion", Float, index=True),
    Column("modelo_riesgo_version", String(120), index=True),
    Column("modelo_duracion_version", String(120), index=True),
    Column("modelo_complicaciones_version", String(120), index=True),
    Column("factors_json", JSON_SQL),
    Column("created_at", DateTime, default=utcnow, nullable=False, index=True),
)


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


def _extract_int(value: Any) -> int:
    if value is None:
        return 0
    m = re.search(r"\d+", str(value))
    if not m:
        return 0
    try:
        return int(m.group(0))
    except Exception:
        return 0


def _clip(value: float, low: float = 0.0, high: float = 1.0) -> float:
    return float(max(low, min(high, float(value))))


def _risk_level(probability: float) -> str:
    p = float(probability)
    if p >= 0.75:
        return "CRITICAL"
    if p >= 0.60:
        return "WARNING"
    return "INFO"


def ensure_quirofano_agent_schema(bind_or_session: Any) -> None:
    bind = getattr(bind_or_session, "bind", None) or bind_or_session
    if bind is None:
        return
    QUIROFANO_AGENT_METADATA.create_all(bind=bind, checkfirst=True)


class BaseAgent:
    def __init__(self, name: str):
        self.name = name

    def cargar_modelo(self, path: str) -> Any:
        if not path:
            return None
        model = load_model_cached(path)
        if model is not None:
            return model
        if joblib is None:
            return None
        # Fallback explícito (compatibilidad).
        try:
            if os.path.exists(path):
                return joblib.load(path)
        except Exception:
            return None
        return None


class QuirofanoAgent(BaseAgent):
    def __init__(self) -> None:
        super().__init__("quirofano")
        self.modelo_riesgo_path = os.getenv("MODELO_RIESGO_V2_PATH", "modelo_riesgo_quirurgico_v2.pkl")
        self.modelo_duracion_path = os.getenv("MODELO_DURACION_QX_PATH", "modelos/pipeline_duracion_qx.pkl")
        self.modelo_complicaciones_path = os.getenv("MODELO_COMPLICACIONES_QX_PATH", "modelos/pipeline_complicaciones_qx.pkl")

        self.risk_bundle = self.cargar_modelo(self.modelo_riesgo_path)
        self.pipeline_riesgo = self._extract_pipeline(self.risk_bundle)
        self.features_riesgo = self._extract_features(self.risk_bundle)
        self.pipeline_duracion = self.cargar_modelo(self.modelo_duracion_path)
        self.pipeline_complicaciones = self.cargar_modelo(self.modelo_complicaciones_path)

    @staticmethod
    def _extract_pipeline(bundle: Any) -> Any:
        if isinstance(bundle, dict) and bundle.get("model") is not None:
            return bundle.get("model")
        return bundle

    @staticmethod
    def _extract_features(bundle: Any) -> List[str]:
        if isinstance(bundle, dict):
            feats = bundle.get("features")
            if isinstance(feats, list):
                return [str(x) for x in feats]
        return []

    @staticmethod
    def _model_version(model: Any, fallback: str) -> str:
        if model is None:
            return "NO_MODEL"
        if isinstance(model, dict) and model.get("version"):
            return _safe_text(model.get("version"))
        return fallback

    @staticmethod
    def _build_raw_features(prog: Any, consulta: Optional[Any]) -> Dict[str, Any]:
        return {
            "edad": int(prog.edad or 0),
            "sexo": _safe_upper(prog.sexo or (consulta.sexo if consulta else "")),
            "ecog": _safe_text(prog.ecog),
            "charlson": _safe_text(prog.charlson),
            "grupo_proc": _safe_text(prog.grupo_procedimiento or prog.procedimiento_programado or prog.procedimiento),
            "abordaje": _safe_text(prog.abordaje),
            "intermed": _safe_upper(prog.requiere_intermed),
        }

    @staticmethod
    def _build_model_vector(raw: Dict[str, Any], features: List[str]) -> List[float]:
        sexo_cod = 1 if _safe_upper(raw.get("sexo")) == "MASCULINO" else 0
        ecog_cod = _extract_int(raw.get("ecog"))
        charlson_cod = _extract_int(raw.get("charlson"))
        intermed_cod = 1 if _safe_upper(raw.get("intermed")) == "SI" else 0
        grupo_proc = _safe_text(raw.get("grupo_proc") or "NO_REGISTRADO")
        abordaje = _safe_text(raw.get("abordaje") or "NO_REGISTRADO")

        out: List[float] = []
        for col in features:
            if col == "edad":
                out.append(float(raw.get("edad") or 0))
            elif col == "sexo_cod":
                out.append(float(sexo_cod))
            elif col == "ecog_cod":
                out.append(float(ecog_cod))
            elif col == "charlson_cod":
                out.append(float(charlson_cod))
            elif col == "intermed_cod":
                out.append(float(intermed_cod))
            elif col.startswith("grupo_"):
                out.append(1.0 if col == f"grupo_{grupo_proc}" else 0.0)
            elif col.startswith("ab_"):
                out.append(1.0 if col == f"ab_{abordaje}" else 0.0)
            else:
                out.append(0.0)
        return out

    def predecir_riesgo_cancelacion(self, prog: Any, consulta: Optional[Any]) -> float:
        raw = self._build_raw_features(prog, consulta)

        model = self.pipeline_riesgo
        if model is not None:
            try:
                if self.features_riesgo:
                    vec = self._build_model_vector(raw, self.features_riesgo)
                    proba = model.predict_proba([vec])[0][1]
                    return _clip(float(proba))

                if pd is not None:
                    df = pd.DataFrame(
                        [
                            {
                                "edad": int(raw["edad"] or 0),
                                "sexo": raw["sexo"],
                                "ecog": raw["ecog"],
                                "charlson": raw["charlson"],
                                "procedimiento": raw["grupo_proc"],
                                "grupo_proc": raw["grupo_proc"],
                                "abordaje": raw["abordaje"],
                                "intermed": raw["intermed"],
                            }
                        ]
                    )
                    proba = model.predict_proba(df)[0][1]
                    return _clip(float(proba))
            except Exception:
                pass

        # Heurística clínica de respaldo.
        risk = 0.10
        if int(raw["edad"] or 0) >= 75:
            risk += 0.10
        if _extract_int(raw["ecog"]) >= 2:
            risk += 0.20
        if _extract_int(raw["charlson"]) >= 5:
            risk += 0.20
        if _safe_upper(raw["intermed"]) == "SI":
            risk += 0.12
        grp = _safe_upper(raw["grupo_proc"])
        if any(x in grp for x in ["CISTOPROSTATECTOMIA", "PROSTATECTOMIA", "NEFRECTOMIA", "NEFROURETERECTOMIA"]):
            risk += 0.15
        if _safe_upper(raw["abordaje"]) in {"ABIERTO + LAPAROSCOPICO", "ABIERTO"}:
            risk += 0.08
        return _clip(risk)

    def predecir_duracion(self, prog: Any, consulta: Optional[Any]) -> float:
        raw = self._build_raw_features(prog, consulta)
        model = self.pipeline_duracion
        if model is not None and pd is not None:
            try:
                df = pd.DataFrame(
                    [
                        {
                            "edad": int(raw["edad"] or 0),
                            "sexo": raw["sexo"],
                            "ecog": raw["ecog"],
                            "charlson": raw["charlson"],
                            "procedimiento": raw["grupo_proc"],
                            "abordaje": raw["abordaje"],
                            "intermed": raw["intermed"],
                        }
                    ]
                )
                pred = float(model.predict(df)[0])
                if pred > 0:
                    return round(pred, 2)
            except Exception:
                pass

        grp = _safe_upper(raw["grupo_proc"])
        if any(x in grp for x in ["CISTOPROSTATECTOMIA", "PROSTATECTOMIA RADICAL + LINFADENECTOMIA"]):
            base = 240.0
        elif any(x in grp for x in ["NEFRECTOMIA", "NEFROURETERECTOMIA"]):
            base = 190.0
        elif any(x in grp for x in ["NEFROLITOTOMIA PERCUTANEA", "ECIRS"]):
            base = 150.0
        elif any(x in grp for x in ["UTIO", "CISTOSTOMIA", "ORQUIECTOMIA", "PENECTOMIA"]):
            base = 75.0
        else:
            base = 120.0

        base += min(_extract_int(raw["ecog"]) * 12.0, 36.0)
        base += min(_extract_int(raw["charlson"]) * 6.0, 36.0)
        if _safe_upper(raw["intermed"]) == "SI":
            base += 20.0
        return round(max(base, 30.0), 2)

    def predecir_riesgo_complicaciones(self, prog: Any, consulta: Optional[Any]) -> float:
        raw = self._build_raw_features(prog, consulta)
        model = self.pipeline_complicaciones
        if model is not None and pd is not None:
            try:
                df = pd.DataFrame(
                    [
                        {
                            "edad": int(raw["edad"] or 0),
                            "sexo": raw["sexo"],
                            "ecog": raw["ecog"],
                            "charlson": raw["charlson"],
                            "procedimiento": raw["grupo_proc"],
                            "abordaje": raw["abordaje"],
                            "intermed": raw["intermed"],
                        }
                    ]
                )
                if hasattr(model, "predict_proba"):
                    return _clip(float(model.predict_proba(df)[0][1]))
                pred = float(model.predict(df)[0])
                return _clip(pred)
            except Exception:
                pass

        risk = 0.08
        ecog = _extract_int(raw["ecog"])
        charlson = _extract_int(raw["charlson"])
        if ecog >= 2:
            risk += 0.18
        if charlson >= 5:
            risk += 0.18
        if int(raw["edad"] or 0) >= 75:
            risk += 0.10
        grp = _safe_upper(raw["grupo_proc"])
        if any(x in grp for x in ["CISTOPROSTATECTOMIA", "NEFRECTOMIA", "NEFROURETERECTOMIA"]):
            risk += 0.15
        return _clip(risk)

    @staticmethod
    def construir_insights(
        prog: Any,
        riesgo_cancelacion: float,
        duracion_estimada_min: float,
        riesgo_complicacion: float,
    ) -> List[Dict[str, Any]]:
        insights: List[Dict[str, Any]] = []
        if riesgo_cancelacion >= 0.65:
            insights.append(
                {
                    "tipo": "RIESGO_CANCELACION",
                    "mensaje": f"Alto riesgo de cancelación ({riesgo_cancelacion:.1%}). Revisar protocolo preoperatorio.",
                    "nivel": "WARNING",
                }
            )
        if riesgo_complicacion >= 0.45:
            insights.append(
                {
                    "tipo": "RIESGO_COMPLICACION",
                    "mensaje": f"Riesgo clínico elevado de complicación ({riesgo_complicacion:.1%}). Intensificar vigilancia.",
                    "nivel": "WARNING",
                }
            )
        if duracion_estimada_min >= 240:
            insights.append(
                {
                    "tipo": "ESTIMACION_DURACION_ALTA",
                    "mensaje": f"Duración estimada alta: {duracion_estimada_min:.0f} minutos.",
                    "nivel": "INFO",
                }
            )
        if _safe_upper(getattr(prog, "protocolo_completo", "")) == "NO":
            insights.append(
                {
                    "tipo": "PROTOCOLO_INCOMPLETO",
                    "mensaje": "Paciente con protocolo incompleto en programación quirúrgica.",
                    "nivel": "WARNING",
                }
            )
        if not insights:
            insights.append(
                {
                    "tipo": "ESTADO_ESTABLE",
                    "mensaje": "Sin alertas críticas detectadas para la programación analizada.",
                    "nivel": "INFO",
                }
            )
        return insights


def _crear_alerta_db(
    sdb: Session,
    *,
    insight: Dict[str, Any],
    programacion_id: int,
    detalles_extra: Optional[Dict[str, Any]] = None,
) -> None:
    detalles = dict(detalles_extra or {})
    detalles.update(
        {
            "generado_por": "QuirofanoAgent_v1",
            "tipo_insight": _safe_text(insight.get("tipo")).upper(),
        }
    )
    sdb.execute(
        insert(ALERTAS).values(
            tipo="QUIROFANO",
            nivel=_safe_upper(insight.get("nivel") or "INFO"),
            mensaje=_safe_text(insight.get("mensaje")),
            programacion_id=int(programacion_id),
            detalles=_dump(detalles),
            fecha_creacion=utcnow(),
            leida=False,
            resuelta=False,
            usuario_asignado=None,
        )
    )


def _guardar_prediccion_db(
    sdb: Session,
    *,
    run_id: Optional[int],
    prog: Any,
    riesgo_cancelacion: float,
    duracion_estimada_min: float,
    riesgo_complicacion: float,
    factors: List[str],
    modelo_riesgo_version: str,
    modelo_duracion_version: str,
    modelo_complicaciones_version: str,
) -> None:
    sdb.execute(
        insert(QUIROFANO_PREDICCIONES).values(
            run_id=run_id,
            programacion_id=int(prog.id),
            consulta_id=int(prog.consulta_id) if prog.consulta_id is not None else None,
            nss=_safe_text(prog.nss),
            paciente=_safe_text(prog.paciente_nombre),
            prediction_date=date.today(),
            riesgo_cancelacion=float(riesgo_cancelacion),
            duracion_estimada_min=float(duracion_estimada_min),
            riesgo_complicacion=float(riesgo_complicacion),
            modelo_riesgo_version=_safe_text(modelo_riesgo_version),
            modelo_duracion_version=_safe_text(modelo_duracion_version),
            modelo_complicaciones_version=_safe_text(modelo_complicaciones_version),
            factors_json=_dump(factors),
            created_at=utcnow(),
        )
    )


def _analisis_programacion(
    db: Session,
    sdb: Session,
    m: Any,
    agent: QuirofanoAgent,
    *,
    prog: Any,
    run_id: Optional[int],
) -> Dict[str, Any]:
    consulta = None
    if getattr(prog, "consulta_id", None):
        consulta = db.query(m.ConsultaDB).filter(m.ConsultaDB.id == int(prog.consulta_id)).first()

    riesgo_cancelacion = agent.predecir_riesgo_cancelacion(prog, consulta)
    duracion_estimada = agent.predecir_duracion(prog, consulta)
    riesgo_complicacion = agent.predecir_riesgo_complicaciones(prog, consulta)
    insights = agent.construir_insights(prog, riesgo_cancelacion, duracion_estimada, riesgo_complicacion)

    alertas_creadas = 0
    for insight in insights:
        nivel = _safe_upper(insight.get("nivel"))
        if nivel in {"WARNING", "CRITICAL"}:
            _crear_alerta_db(
                sdb,
                insight=insight,
                programacion_id=int(prog.id),
                detalles_extra={
                    "consulta_id": int(prog.consulta_id) if prog.consulta_id is not None else None,
                    "riesgo_cancelacion": round(riesgo_cancelacion, 4),
                    "riesgo_complicacion": round(riesgo_complicacion, 4),
                    "duracion_estimada_min": round(duracion_estimada, 2),
                },
            )
            alertas_creadas += 1

    factors = []
    if _extract_int(getattr(prog, "ecog", 0)) >= 2:
        factors.append("ECOG>=2")
    if _extract_int(getattr(prog, "charlson", 0)) >= 5:
        factors.append("Charlson>=5")
    if int(getattr(prog, "edad", 0) or 0) >= 75:
        factors.append("Edad>=75")
    if _safe_upper(getattr(prog, "requiere_intermed", "")) == "SI":
        factors.append("Requiere INTERMED")
    if not factors:
        factors.append("Sin factores dominantes")

    _guardar_prediccion_db(
        sdb,
        run_id=run_id,
        prog=prog,
        riesgo_cancelacion=riesgo_cancelacion,
        duracion_estimada_min=duracion_estimada,
        riesgo_complicacion=riesgo_complicacion,
        factors=factors,
        modelo_riesgo_version=agent._model_version(agent.risk_bundle, "heuristic_risk_v1"),
        modelo_duracion_version=agent._model_version(agent.pipeline_duracion, "heuristic_duration_v1"),
        modelo_complicaciones_version=agent._model_version(agent.pipeline_complicaciones, "heuristic_complications_v1"),
    )

    return {
        "ok": True,
        "programacion_id": int(prog.id),
        "consulta_id": int(prog.consulta_id) if prog.consulta_id is not None else None,
        "nss": _safe_text(prog.nss),
        "paciente": _safe_text(prog.paciente_nombre),
        "procedimiento": _safe_text(prog.procedimiento_programado or prog.procedimiento),
        "riesgo_cancelacion": round(float(riesgo_cancelacion), 4),
        "duracion_estimada_min": round(float(duracion_estimada), 2),
        "riesgo_complicacion": round(float(riesgo_complicacion), 4),
        "insights": insights,
        "alertas_creadas": int(alertas_creadas),
    }


def analyze_quirofano_programacion(
    db: Session,
    sdb: Session,
    m: Any,
    *,
    programacion_id: int,
    run_id: Optional[int] = None,
) -> Dict[str, Any]:
    ensure_quirofano_agent_schema(sdb)
    agent = QuirofanoAgent()
    prog = sdb.query(m.SurgicalProgramacionDB).filter(m.SurgicalProgramacionDB.id == int(programacion_id)).first()
    if not prog:
        return {"ok": False, "error": "Programación no encontrada", "programacion_id": int(programacion_id)}

    result = _analisis_programacion(db, sdb, m, agent, prog=prog, run_id=run_id)
    sdb.commit()
    return result


def analyze_quirofano_programaciones(
    db: Session,
    sdb: Session,
    m: Any,
    *,
    window_days: int = 30,
    run_id: Optional[int] = None,
    limit: int = 400,
) -> Dict[str, Any]:
    ensure_quirofano_agent_schema(sdb)
    agent = QuirofanoAgent()
    since = date.today() - timedelta(days=max(1, min(int(window_days or 30), 365)))

    rows = (
        sdb.query(m.SurgicalProgramacionDB)
        .filter(m.SurgicalProgramacionDB.fecha_programada >= since)
        .order_by(m.SurgicalProgramacionDB.fecha_programada.desc(), m.SurgicalProgramacionDB.id.desc())
        .limit(max(1, min(int(limit or 400), 3000)))
        .all()
    )

    analyses: List[Dict[str, Any]] = []
    total_alertas = 0
    high_risk_cancel = 0
    high_risk_comp = 0
    durations: List[float] = []
    by_proc = Counter()

    for prog in rows:
        result = _analisis_programacion(db, sdb, m, agent, prog=prog, run_id=run_id)
        analyses.append(result)
        total_alertas += int(result.get("alertas_creadas") or 0)
        if float(result.get("riesgo_cancelacion") or 0.0) >= 0.65:
            high_risk_cancel += 1
        if float(result.get("riesgo_complicacion") or 0.0) >= 0.45:
            high_risk_comp += 1
        durations.append(float(result.get("duracion_estimada_min") or 0.0))
        by_proc[_safe_upper(result.get("procedimiento") or "NO_REGISTRADO")] += 1

    sdb.commit()

    n = len(analyses)
    metrics = {
        "period_start": since.isoformat(),
        "period_end": date.today().isoformat(),
        "total_programaciones_analizadas": n,
        "alertas_creadas": total_alertas,
        "alto_riesgo_cancelacion": high_risk_cancel,
        "alto_riesgo_complicacion": high_risk_comp,
        "duracion_estimada_media_min": round((sum(durations) / n), 2) if n else 0.0,
        "duracion_estimada_mediana_min": round(float(sorted(durations)[n // 2]), 2) if n else 0.0,
        "top_procedimientos": dict(by_proc.most_common(20)),
    }

    insights: List[Dict[str, Any]] = []
    if n > 0 and (high_risk_cancel / float(n)) >= 0.25:
        insights.append(
            {
                "type": "cancelacion_alta",
                "severity": "ALTA",
                "text": ">=25% de programaciones analizadas con riesgo alto de cancelación.",
            }
        )
    if n > 0 and (high_risk_comp / float(n)) >= 0.20:
        insights.append(
            {
                "type": "complicacion_alta",
                "severity": "ALTA",
                "text": ">=20% de programaciones analizadas con riesgo alto de complicación.",
            }
        )
    if metrics["duracion_estimada_media_min"] >= 180:
        insights.append(
            {
                "type": "duracion_promedio_alta",
                "severity": "MEDIA",
                "text": "Duración estimada media >= 180 min. Ajustar planeación de tiempos de sala.",
            }
        )

    return {
        "ok": True,
        "window_days": int(window_days),
        "run_id": run_id,
        "metrics": metrics,
        "insights": insights,
        "sample": analyses[: min(25, len(analyses))],
    }


def list_recent_quirofano_alerts(
    sdb: Session,
    *,
    limit: int = 200,
    days: int = 30,
    level: Optional[str] = None,
    only_open: bool = False,
) -> List[Dict[str, Any]]:
    ensure_quirofano_agent_schema(sdb)
    since = utcnow() - timedelta(days=max(1, min(int(days or 30), 365)))
    q = select(ALERTAS).where(
        and_(
            ALERTAS.c.tipo == "QUIROFANO",
            ALERTAS.c.fecha_creacion >= since,
        )
    )
    if level:
        q = q.where(ALERTAS.c.nivel == _safe_upper(level))
    if only_open:
        q = q.where(ALERTAS.c.resuelta.is_(False))
    rows = sdb.execute(q.order_by(desc(ALERTAS.c.id)).limit(max(1, min(int(limit or 200), 2000)))).mappings().all()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": int(r["id"]),
                "tipo": _safe_text(r["tipo"]),
                "nivel": _safe_upper(r["nivel"]),
                "mensaje": _safe_text(r["mensaje"]),
                "programacion_id": r["programacion_id"],
                "detalles": _load(r["detalles"], {}),
                "fecha_creacion": r["fecha_creacion"].isoformat() if r["fecha_creacion"] else None,
                "leida": bool(r["leida"]),
                "resuelta": bool(r["resuelta"]),
                "usuario_asignado": _safe_text(r["usuario_asignado"]),
            }
        )
    return out


def summarize_quirofano_alerts(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    by_level = Counter(_safe_upper(x.get("nivel")) for x in rows)
    by_type = Counter(_safe_upper(x.get("detalles", {}).get("tipo_insight") or "NO_TIPO") for x in rows)
    return {
        "total": len(rows),
        "por_nivel": dict(by_level),
        "por_tipo_insight": dict(by_type),
    }


def list_quirofano_predictions(
    sdb: Session,
    *,
    programacion_id: Optional[int] = None,
    consulta_id: Optional[int] = None,
    limit: int = 300,
) -> List[Dict[str, Any]]:
    ensure_quirofano_agent_schema(sdb)
    q = select(QUIROFANO_PREDICCIONES)
    if programacion_id is not None:
        q = q.where(QUIROFANO_PREDICCIONES.c.programacion_id == int(programacion_id))
    if consulta_id is not None:
        q = q.where(QUIROFANO_PREDICCIONES.c.consulta_id == int(consulta_id))
    rows = sdb.execute(
        q.order_by(desc(QUIROFANO_PREDICCIONES.c.id)).limit(max(1, min(int(limit or 300), 2000)))
    ).mappings().all()
    return [
        {
            "id": int(r["id"]),
            "run_id": r["run_id"],
            "programacion_id": r["programacion_id"],
            "consulta_id": r["consulta_id"],
            "nss": _safe_text(r["nss"]),
            "paciente": _safe_text(r["paciente"]),
            "prediction_date": r["prediction_date"].isoformat() if r["prediction_date"] else None,
            "riesgo_cancelacion": r["riesgo_cancelacion"],
            "duracion_estimada_min": r["duracion_estimada_min"],
            "riesgo_complicacion": r["riesgo_complicacion"],
            "modelo_riesgo_version": _safe_text(r["modelo_riesgo_version"]),
            "modelo_duracion_version": _safe_text(r["modelo_duracion_version"]),
            "modelo_complicaciones_version": _safe_text(r["modelo_complicaciones_version"]),
            "factors": _load(r["factors_json"], []),
            "created_at": r["created_at"].isoformat() if r["created_at"] else None,
        }
        for r in rows
    ]
