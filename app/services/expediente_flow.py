import json
import logging
import re
from collections import defaultdict
from datetime import date, datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import quote, unquote

from fastapi import Request
from fastapi.responses import HTMLResponse
from sqlalchemy import String, and_, cast, func, or_, select
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _norm_nss(value: Any) -> str:
    return re.sub(r"\D", "", _safe_text(value))[:10]


def _norm_name(value: Any) -> str:
    txt = _safe_text(value).upper()
    txt = re.sub(r"\s+", " ", txt)
    return txt


def _to_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        return None


def _day_iso_from_dt(value: Any) -> str:
    txt = _safe_text(value)
    if not txt:
        return ""
    try:
        return datetime.fromisoformat(txt.replace("Z", "+00:00")).date().isoformat()
    except Exception:
        return txt[:10] if len(txt) >= 10 else ""


def _nss_compat_expr(column: Any, target_nss: str) -> Optional[Any]:
    """Compatibilidad NSS: soporta histórico (11+ dígitos/separadores) y NSS_10."""
    nss = _norm_nss(target_nss)
    if not nss:
        return None
    col_txt = func.coalesce(cast(column, String), "")
    col_digits = func.replace(
        func.replace(
            func.replace(
                func.replace(col_txt, " ", ""),
                "-",
                "",
            ),
            "/",
            "",
        ),
        ".",
        "",
    )
    return or_(
        func.substr(col_digits, 1, 10) == nss,
        col_txt == nss,
        col_txt.like(f"{nss}%"),
    )


def _name_compat_expr(column: Any, target_name: str) -> Optional[Any]:
    name = _norm_name(target_name)
    if not name:
        return None
    col_txt = func.upper(func.coalesce(cast(column, String), ""))
    return col_txt.like(f"%{name}%")


def _or_compat(*exprs: Any) -> Optional[Any]:
    valid = [e for e in exprs if e is not None]
    if not valid:
        return None
    if len(valid) == 1:
        return valid[0]
    return or_(*valid)


def _json_dict(value: Any) -> Dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        try:
            obj = json.loads(value)
            if isinstance(obj, dict):
                return obj
        except Exception:
            return {}
    return {}


def _json_list(value: Any) -> List[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return value
    if isinstance(value, str):
        try:
            obj = json.loads(value)
            if isinstance(obj, list):
                return obj
        except Exception:
            return []
    return []


def _match_identity(
    *,
    record_nss: Any,
    record_name: Any,
    target_nss: str,
    target_name: str,
) -> bool:
    row_nss = _norm_nss(record_nss)
    row_name = _norm_name(record_name)
    if target_nss and row_nss and target_nss == row_nss:
        return True
    if target_name and row_name:
        if target_name in row_name or row_name in target_name:
            return True
    return False


def _extract_lab_markers_from_payload(payload: Dict[str, Any]) -> Dict[str, float]:
    markers: Dict[str, float] = {}
    map_keys = {
        "creatinina": ["CREATININA", "CR", "CREATININA MG/DL"],
        "hemoglobina": ["HEMOGLOBINA", "HB", "HGB"],
        "leucocitos": ["LEUCOCITOS", "LEUCOS", "WBC"],
        "plaquetas": ["PLAQUETAS", "PLT"],
        "sodio": ["SODIO", "NA"],
        "potasio": ["POTASIO", "K"],
    }
    for out_key, keys in map_keys.items():
        value_num: Optional[float] = None
        for key in keys:
            raw = _safe_text(payload.get(key))
            if not raw:
                continue
            match = re.search(r"-?\d+(?:\.\d+)?", raw.replace(",", ""))
            if not match:
                continue
            try:
                value_num = float(match.group(0))
            except Exception:
                value_num = None
            if value_num is not None:
                break
        if value_num is not None:
            markers[out_key] = value_num
    return markers


def _build_profile_candidates(
    db: Session,
    m: Any,
    *,
    q: Optional[str],
    nss: Optional[str],
    nombre: Optional[str],
) -> List[Dict[str, Any]]:
    target_q = _safe_text(q)
    target_nss = _norm_nss(nss or target_q)
    target_nombre = _norm_name(nombre or target_q)

    candidates: Dict[str, Dict[str, Any]] = {}

    def _upsert_candidate(*, nss_value: Any, nombre_value: Any, consulta_id: Optional[int], sexo: Any, hgz: Any):
        nss_norm = _norm_nss(nss_value)
        nombre_norm = _norm_name(nombre_value)
        if not nss_norm and not nombre_norm:
            return
        key = nss_norm or f"NAME:{nombre_norm}"
        item = candidates.get(key)
        if item is None:
            item = {
                "key": key,
                "nss": nss_norm,
                "nombre": nombre_norm,
                "consulta_id": consulta_id,
                "sexo": _safe_text(sexo).upper(),
                "hgz": _safe_text(hgz).upper(),
                "sources": set(),
            }
            candidates[key] = item
        if consulta_id and not item.get("consulta_id"):
            item["consulta_id"] = consulta_id
        if not item.get("sexo") and sexo:
            item["sexo"] = _safe_text(sexo).upper()
        if not item.get("hgz") and hgz:
            item["hgz"] = _safe_text(hgz).upper()

    # Consultas
    consulta_query = db.query(m.ConsultaDB)
    if target_nss:
        consulta_query = consulta_query.filter(_nss_compat_expr(m.ConsultaDB.nss, target_nss))
    elif target_nombre:
        consulta_query = consulta_query.filter(_name_compat_expr(m.ConsultaDB.nombre, target_nombre))
    elif target_q:
        consulta_query = consulta_query.filter(
            _or_compat(
                _name_compat_expr(m.ConsultaDB.nombre, target_q),
                m.ConsultaDB.curp.contains(target_q.upper()),
                _nss_compat_expr(m.ConsultaDB.nss, _norm_nss(target_q)),
            )
        )
    else:
        consulta_query = consulta_query.order_by(m.ConsultaDB.id.desc())

    for row in consulta_query.order_by(m.ConsultaDB.id.desc()).limit(80).all():
        _upsert_candidate(
            nss_value=row.nss,
            nombre_value=row.nombre,
            consulta_id=row.id,
            sexo=row.sexo,
            hgz=getattr(row, "hgz", None),
        )
        candidates[_norm_nss(row.nss) or f"NAME:{_norm_name(row.nombre)}"]["sources"].add("consulta")

    # Hospitalizaciones
    hosp_query = db.query(m.HospitalizacionDB)
    if target_nss:
        hosp_query = hosp_query.filter(_nss_compat_expr(m.HospitalizacionDB.nss, target_nss))
    elif target_nombre:
        hosp_query = hosp_query.filter(_name_compat_expr(m.HospitalizacionDB.nombre_completo, target_nombre))
    elif target_q:
        hosp_query = hosp_query.filter(
            _or_compat(
                _name_compat_expr(m.HospitalizacionDB.nombre_completo, target_q),
                _nss_compat_expr(m.HospitalizacionDB.nss, _norm_nss(target_q)),
            )
        )
    for row in hosp_query.order_by(m.HospitalizacionDB.id.desc()).limit(120).all():
        _upsert_candidate(
            nss_value=row.nss,
            nombre_value=row.nombre_completo,
            consulta_id=row.consulta_id,
            sexo=row.sexo,
            hgz=row.hgz_envio,
        )
        candidates[_norm_nss(row.nss) or f"NAME:{_norm_name(row.nombre_completo)}"]["sources"].add("hospitalizacion")

    # Master identity anclado a NSS (fase 1).
    try:
        from app.services.master_identity_flow import (
            PATIENT_MASTER_IDENTITY,
            ensure_master_identity_schema,
            normalize_nss_10,
        )

        ensure_master_identity_schema(db)
        mi_query = select(PATIENT_MASTER_IDENTITY)
        nss_q = normalize_nss_10(target_nss or target_q)
        if nss_q:
            mi_query = mi_query.where(PATIENT_MASTER_IDENTITY.c.nss_canonico == nss_q)
        elif target_nombre:
            mi_query = mi_query.where(PATIENT_MASTER_IDENTITY.c.nombre_canonico.contains(target_nombre))
        elif target_q:
            mi_query = mi_query.where(
                or_(
                    PATIENT_MASTER_IDENTITY.c.nss_canonico.contains(_norm_nss(target_q)),
                    PATIENT_MASTER_IDENTITY.c.nombre_canonico.contains(target_nombre or _norm_name(target_q)),
                )
            )
        mi_rows = db.execute(mi_query.limit(120)).mappings().all()
        for row in mi_rows:
            _upsert_candidate(
                nss_value=row.get("nss_canonico"),
                nombre_value=row.get("nombre_canonico"),
                consulta_id=row.get("ultima_consulta_id"),
                sexo=row.get("sexo_canonico"),
                hgz=None,
            )
            candidates[_norm_nss(row.get("nss_canonico")) or f"NAME:{_norm_name(row.get('nombre_canonico'))}"]["sources"].add(
                "master_identity"
            )
    except Exception:
        logger.debug("No se pudo consultar master_identity durante búsqueda de perfil", exc_info=True)

    out: List[Dict[str, Any]] = []
    for item in candidates.values():
        source_list = sorted(item["sources"])
        out.append(
            {
                "key": item["key"],
                "nss": item["nss"] or "",
                "nombre": item["nombre"] or "",
                "consulta_id": item.get("consulta_id"),
                "sexo": item.get("sexo") or "NO_REGISTRADO",
                "hgz": item.get("hgz") or "NO_REGISTRADO",
                "sources": source_list,
            }
        )
    out.sort(key=lambda x: (0 if x.get("nss") else 1, x.get("nombre") or ""))
    return out[:120]


def _select_consulta(
    db: Session,
    m: Any,
    *,
    consulta_id: Optional[int],
    curp: Optional[str],
    nss: Optional[str],
    nombre: Optional[str],
) -> Optional[Any]:
    if consulta_id:
        return db.query(m.ConsultaDB).filter(m.ConsultaDB.id == consulta_id).first()
    if curp:
        return (
            db.query(m.ConsultaDB)
            .filter(m.ConsultaDB.curp == m.normalize_curp(curp))
            .order_by(m.ConsultaDB.id.desc())
            .first()
        )
    if nss:
        nss_norm = _norm_nss(nss)
        if nss_norm:
            return (
                db.query(m.ConsultaDB)
                .filter(_nss_compat_expr(m.ConsultaDB.nss, nss_norm))
                .order_by(m.ConsultaDB.id.desc())
                .first()
            )
    if nombre:
        nombre_norm = _norm_name(nombre)
        if nombre_norm:
            return (
                db.query(m.ConsultaDB)
                .filter(_name_compat_expr(m.ConsultaDB.nombre, nombre_norm))
                .order_by(m.ConsultaDB.id.desc())
                .first()
            )
    return None


def _build_timeline(
    *,
    m: Any,
    consulta_ids: List[int],
    target_nss: str,
    target_name: str,
    hospitalizaciones: List[Any],
    guardia_rows: List[Dict[str, Any]],
    censo_rows: List[Any],
    quirofano_rows: List[Any],
    surgical_rows: List[Any],
    postq_rows: List[Any],
    lab_rows: List[Any],
) -> Dict[str, Any]:
    day_map: Dict[str, Dict[str, Any]] = defaultdict(
        lambda: {
            "censo": [],
            "valoraciones": [],
            "hallazgos_qx": [],
            "otros": [],
            "labs": {},
        }
    )

    for row in guardia_rows:
        payload = _json_dict(row.get("payload_json"))
        day = row.get("fecha")
        if day is None:
            continue
        key = day.isoformat()
        dataset = _safe_text(row.get("dataset")).lower()
        bucket = day_map[key]

        if dataset == "laboratorios":
            markers = _extract_lab_markers_from_payload(payload)
            bucket["labs"].update(markers)
            continue
        if dataset == "valoraciones":
            txt = _safe_text(payload.get("TEXTO VALORACION")) or _safe_text(payload.get("OBSERVACIONES"))
            if txt:
                bucket["valoraciones"].append(txt[:260])
            continue
        if dataset == "operados":
            proc = _safe_text(payload.get("PROCEDIMIENTO REALIZADO") or payload.get("PROCEDIMIENTO PROGRAMADO"))
            hall = _safe_text(payload.get("HALLAZGOS"))
            summary = f"{proc} | {hall}".strip(" |")
            if summary:
                bucket["hallazgos_qx"].append(summary[:260])
            continue
        if dataset == "censo":
            cama = _safe_text(payload.get("CAMA"))
            estado = _safe_text(payload.get("ESTADO DE SALUD"))
            info = "Censo"
            if cama:
                info += f" cama {cama}"
            if estado:
                info += f" ({estado})"
            bucket["censo"].append(info)
            continue
        if dataset == "ingresos":
            plan = _safe_text(payload.get("PLAN"))
            dx = _safe_text(payload.get("DIAGNOSTICO"))
            bucket["otros"].append(f"Ingreso guardia: {dx} {plan}".strip())
            continue

    for snap in censo_rows:
        day = snap.fecha
        if day is None:
            continue
        key = day.isoformat()
        pacientes = _json_list(getattr(snap, "pacientes_json", None))
        for p in pacientes:
            if not isinstance(p, dict):
                continue
            if _match_identity(
                record_nss=p.get("nss"),
                record_name=p.get("nombre_completo"),
                target_nss=target_nss,
                target_name=target_name,
            ):
                cama = _safe_text(p.get("cama"))
                estado = _safe_text(p.get("estado_clinico") or p.get("estatus_detalle"))
                msg = "Censo diario"
                if cama:
                    msg += f" cama {cama}"
                if estado:
                    msg += f" ({estado})"
                day_map[key]["censo"].append(msg)
                break

    for q in quirofano_rows:
        d = q.fecha_realizacion or q.fecha_programada
        if d is None:
            continue
        key = d.isoformat()
        proc = _safe_text(q.procedimiento).upper()
        est = _safe_text(q.estatus).upper() or "PROGRAMADA"
        msg = f"{est}: {proc}" if proc else est
        day_map[key]["hallazgos_qx"].append(msg)

    for q in surgical_rows:
        d = q.fecha_realizacion or q.fecha_programada
        if d is None:
            continue
        key = d.isoformat()
        proc = _safe_text(q.procedimiento_programado or q.procedimiento).upper()
        est = _safe_text(q.estatus).upper() or "PROGRAMADA"
        if proc:
            day_map[key]["hallazgos_qx"].append(f"{est} (dedicada): {proc}")

    for p in postq_rows:
        d = p.fecha_realizacion
        if d is None:
            continue
        key = d.isoformat()
        cir = _safe_text(p.cirujano).upper()
        sangrado = p.sangrado_ml
        dx_post = _safe_text(getattr(p, "diagnostico_postop", None))
        proc_post = _safe_text(getattr(p, "procedimiento_realizado", None))
        hall_post = _safe_text(getattr(p, "complicaciones", None)) or _safe_text(getattr(p, "nota_postquirurgica", None))
        msg = f"PostQx {cir}" if cir else "PostQx"
        if dx_post:
            msg += f" | Dx {dx_post}"
        if proc_post:
            msg += f" | Proc {proc_post}"
        if sangrado is not None:
            msg += f" | Sangrado {sangrado} mL"
        if hall_post:
            msg += f" | Hallazgos {hall_post[:140]}"
        day_map[key]["hallazgos_qx"].append(msg)

    for lab in lab_rows:
        ts = getattr(lab, "timestamp", None)
        if ts is None:
            continue
        key = ts.date().isoformat()
        test_name = _safe_text(getattr(lab, "test_name", None)).lower()
        test_code = _safe_text(getattr(lab, "test_code", None)).lower()
        value_raw = _safe_text(getattr(lab, "value", None)).replace(",", "")
        match = re.search(r"-?\d+(?:\.\d+)?", value_raw)
        if not match:
            continue
        try:
            value_num = float(match.group(0))
        except Exception:
            continue

        marker = None
        joined = f"{test_name} {test_code}"
        if "creatin" in joined or joined.strip() == "cr":
            marker = "creatinina"
        elif "hemoglob" in joined or joined.strip().startswith("hb") or "hgb" in joined:
            marker = "hemoglobina"
        elif "leuco" in joined or "wbc" in joined:
            marker = "leucocitos"
        elif "plaquet" in joined or "plt" in joined:
            marker = "plaquetas"
        elif "sodio" in joined or joined.strip() == "na":
            marker = "sodio"
        elif "potasio" in joined or joined.strip() == "k":
            marker = "potasio"

        if marker:
            day_map[key]["labs"][marker] = value_num

    # Construcción por hospitalización
    episodes: List[Dict[str, Any]] = []
    for idx, hosp in enumerate(hospitalizaciones):
        start = hosp.fecha_ingreso or date.today()
        end = hosp.fecha_egreso or date.today()
        if end < start:
            end = start
        days: List[Dict[str, Any]] = []
        cursor = start
        while cursor <= end:
            key = cursor.isoformat()
            bucket = day_map.get(key, {})
            labs = dict(bucket.get("labs") or {})
            alerts: List[str] = []
            cr = labs.get("creatinina")
            hb = labs.get("hemoglobina")
            leu = labs.get("leucocitos")
            plt = labs.get("plaquetas")
            na = labs.get("sodio")
            k = labs.get("potasio")
            if cr is not None and cr >= 2.0:
                alerts.append("IRA probable (Cr>=2.0)")
            if hb is not None and hb < 8.0:
                alerts.append("Hb < 8")
            if leu is not None and leu > 10000:
                alerts.append("Leucocitos > 10,000")
            if plt is not None and plt < 150:
                alerts.append("Plaquetas < 150")
            if na is not None and (na < 135 or na > 145):
                alerts.append("Disnatremia")
            if k is not None and (k < 3.5 or k > 5.0):
                alerts.append("Dispotasemia")

            days.append(
                {
                    "fecha": key,
                    "dia_estancia": (cursor - start).days + 1,
                    "censo": bucket.get("censo", []),
                    "hallazgos_qx": bucket.get("hallazgos_qx", []),
                    "valoraciones": bucket.get("valoraciones", []),
                    "otros": bucket.get("otros", []),
                    "labs": labs,
                    "alerts": alerts,
                }
            )
            cursor += timedelta(days=1)

        episodes.append(
            {
                "id": hosp.id,
                "tipo": "PRIMERA HOSPITALIZACIÓN" if idx == 0 else f"HOSPITALIZACIÓN PREVIA #{idx}",
                "fecha_ingreso": start.isoformat() if start else "",
                "fecha_egreso": end.isoformat() if end else "",
                "dias": len(days),
                "diagnostico": _safe_text(hosp.diagnostico),
                "ingreso_tipo": _safe_text(hosp.ingreso_tipo),
                "urgencia_tipo": _safe_text(hosp.urgencia_tipo),
                "hgz_envio": _safe_text(hosp.hgz_envio),
                "timeline": days,
            }
        )

    # Si no hay episodios, mostrar cronología por fechas con eventos detectados.
    timeline_general: List[Dict[str, Any]] = []
    if not episodes and day_map:
        for key in sorted(day_map.keys()):
            bucket = day_map[key]
            timeline_general.append(
                {
                    "fecha": key,
                    "censo": bucket.get("censo", []),
                    "hallazgos_qx": bucket.get("hallazgos_qx", []),
                    "valoraciones": bucket.get("valoraciones", []),
                    "otros": bucket.get("otros", []),
                    "labs": bucket.get("labs", {}),
                }
            )

    return {
        "episodes": episodes,
        "timeline_general": timeline_general,
    }


def _build_flujo_clinico_view(
    *,
    consulta_exists: bool,
    hospitalizaciones_payload: List[Dict[str, Any]],
    cirugias_eventos: List[Dict[str, Any]],
    flow_event_rows: List[Any],
    active_hospitalizacion: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    def _event_flag(*names: str) -> bool:
        targets = {n.upper() for n in names}
        for e in flow_event_rows:
            if _safe_text(getattr(e, "evento", None)).upper() in targets:
                return True
        return False

    has_hosp_ingreso = _event_flag("HOSP_INGRESO") or len(hospitalizaciones_payload) > 0
    has_qx_realizada = _event_flag("REALIZADA") or any(
        _safe_text(x.get("estatus")).upper() == "REALIZADA" for x in cirugias_eventos
    )
    has_hosp_egreso = _event_flag("HOSP_EGRESO") or any(_safe_text(h.get("fecha_egreso")) for h in hospitalizaciones_payload)
    has_urg_programada = _event_flag("URG_PROGRAMADA") or any(
        _safe_text(x.get("origen")).upper() == "URGENCIA" for x in cirugias_eventos
    )

    steps = [
        {"key": "consulta", "label": "Consulta registrada", "done": bool(consulta_exists)},
        {"key": "urgencia", "label": "Urgencia programada", "done": bool(has_urg_programada)},
        {"key": "hospitalizacion", "label": "Hospitalización iniciada", "done": bool(has_hosp_ingreso)},
        {"key": "cirugia", "label": "Cirugía realizada", "done": bool(has_qx_realizada)},
        {"key": "egreso", "label": "Egreso hospitalario", "done": bool(has_hosp_egreso)},
    ]

    estado_actual = "CONSULTA"
    if has_urg_programada:
        estado_actual = "URG_PROGRAMADA"
    if has_hosp_ingreso:
        estado_actual = "HOSPITALIZACION"
    if has_qx_realizada:
        estado_actual = "CIRUGIA_REALIZADA"
    if has_hosp_egreso:
        estado_actual = "EGRESADO"
    if active_hospitalizacion and active_hospitalizacion.get("id"):
        estado_actual = "HOSPITALIZACION_ACTIVA"

    journey: List[Dict[str, Any]] = []
    for ev in flow_event_rows:
        created = getattr(ev, "creado_en", None)
        created_txt = ""
        if isinstance(created, datetime):
            created_txt = created.strftime("%Y-%m-%d %H:%M")
        elif isinstance(created, date):
            created_txt = created.isoformat()
        metadata = {}
        try:
            metadata_raw = getattr(ev, "metadata_json", None)
            if isinstance(metadata_raw, dict):
                metadata = metadata_raw
            elif metadata_raw:
                metadata = json.loads(metadata_raw)
        except Exception:
            metadata = {}
        resume_parts = [
            _safe_text(getattr(ev, "diagnostico", None)),
            _safe_text(getattr(ev, "procedimiento", None)),
        ]
        if _safe_text(getattr(ev, "hgz", None)):
            resume_parts.append(f"HGZ {_safe_text(getattr(ev, 'hgz', None))}")
        if metadata.get("cama"):
            resume_parts.append(f"CAMA {metadata.get('cama')}")
        summary = " | ".join([p for p in resume_parts if p])
        journey.append(
            {
                "fecha": created_txt,
                "evento": _safe_text(getattr(ev, "evento", None)).upper() or "NO_REGISTRADO",
                "estatus": _safe_text(getattr(ev, "estatus", None)).upper() or "NO_REGISTRADO",
                "summary": summary,
            }
        )

    if not journey:
        for h in hospitalizaciones_payload:
            if h.get("fecha_ingreso"):
                journey.append(
                    {
                        "fecha": h.get("fecha_ingreso"),
                        "evento": "HOSP_INGRESO",
                        "estatus": "ACTIVO",
                        "summary": f"{h.get('diagnostico') or ''} | CAMA {h.get('cama') or 'N/E'}",
                    }
                )
            if h.get("fecha_egreso"):
                journey.append(
                    {
                        "fecha": h.get("fecha_egreso"),
                        "evento": "HOSP_EGRESO",
                        "estatus": "EGRESADO",
                        "summary": f"{h.get('diagnostico') or ''} | CAMA {h.get('cama') or 'N/E'}",
                    }
                )
        for c in cirugias_eventos:
            if c.get("fecha"):
                journey.append(
                    {
                        "fecha": c.get("fecha"),
                        "evento": "REALIZADA" if _safe_text(c.get("estatus")).upper() == "REALIZADA" else "PROGRAMADA",
                        "estatus": _safe_text(c.get("estatus")).upper() or "NO_REGISTRADO",
                        "summary": f"{c.get('diagnostico') or ''} | {c.get('procedimiento') or ''}",
                    }
                )

    journey.sort(key=lambda x: x.get("fecha") or "")
    return {
        "estado_actual": estado_actual,
        "steps": steps,
        "journey": journey[-200:],
    }


async def ver_expediente_flow(
    request: Request,
    consulta_id: Optional[int],
    curp: Optional[str],
    db: Session,
    *,
    nss: Optional[str] = None,
    nombre: Optional[str] = None,
    q: Optional[str] = None,
) -> Any:
    from app.core.app_context import main_proxy as m
    from app.services.hospital_guardia_flow import (
        HOSP_GUARDIA_REGISTROS,
        ensure_hospital_guardia_schema,
    )
    from app.services.expediente_plus_flow import (
        ensure_default_rule,
        get_enriched_by_consulta_id,
        log_expediente_access,
    )
    from app.services.expediente_nota_medica_flow import (
        ensure_expediente_nota_schema,
        get_active_hospitalizacion_for_profile,
        list_notas_medicas_profile,
        list_profile_alertas,
    )
    from app.services.hospitalization_notes_flow import (
        ensure_hospitalization_notes_schema,
        get_active_episode_by_patient,
        list_patient_daily_notes,
        list_patient_episodes,
        summarize_patient_episodes,
    )
    from app.services.consulta_aditivos_flow import (
        ensure_consulta_aditivos_schema,
        fetch_consulta_studies,
    )
    from app.services.inpatient_labs_notes_service import (
        list_tags as list_inpatient_tags_v2,
    )
    from app.services.ui_context_flow import get_active_context, save_active_context

    ensure_hospital_guardia_schema(db)
    ensure_expediente_nota_schema(db)
    ensure_hospitalization_notes_schema(db)
    ensure_consulta_aditivos_schema(db)
    ensure_default_rule(db)

    if not consulta_id and not _safe_text(nss) and not _safe_text(nombre) and not _safe_text(q):
        try:
            raw_ctx = _safe_text(request.cookies.get("rnp_patient_context"))
            if raw_ctx:
                ctx = json.loads(unquote(raw_ctx))
                if isinstance(ctx, dict):
                    if consulta_id is None and _safe_text(ctx.get("consulta_id")).isdigit():
                        consulta_id = int(_safe_text(ctx.get("consulta_id")))
                    nss = nss or _safe_text(ctx.get("nss"))
                    nombre = nombre or _safe_text(ctx.get("nombre"))
        except Exception:
            logger.debug("No se pudo recuperar contexto activo por cookie en expediente", exc_info=True)

    if not consulta_id and not _safe_text(nss) and not _safe_text(nombre) and not _safe_text(q):
        try:
            actor = _safe_text(request.headers.get("X-User") or "ui_shell")
            ctx = get_active_context(db, actor=actor)
            if isinstance(ctx, dict):
                if consulta_id is None and _safe_text(ctx.get("consulta_id")).isdigit():
                    consulta_id = int(_safe_text(ctx.get("consulta_id")))
                nss = nss or _safe_text(ctx.get("nss"))
                nombre = nombre or _safe_text(ctx.get("nombre"))
        except Exception:
            logger.debug("No se pudo recuperar contexto activo persistente en expediente", exc_info=True)

    # Candidatos para buscador inteligente (NSS / Nombre)
    profile_candidates = _build_profile_candidates(
        db,
        m,
        q=q,
        nss=nss,
        nombre=nombre,
    )

    consulta = _select_consulta(
        db,
        m,
        consulta_id=consulta_id,
        curp=curp,
        nss=nss,
        nombre=nombre,
    )

    if consulta is None and not consulta_id and not curp and profile_candidates:
        req_nss = _norm_nss(nss or q)
        req_name = _norm_name(nombre or q)
        selected_candidate: Optional[Dict[str, Any]] = None

        for cand in profile_candidates:
            cand_nss = _norm_nss(cand.get("nss"))
            cand_name = _norm_name(cand.get("nombre"))
            if req_nss and cand_nss and req_nss == cand_nss:
                selected_candidate = cand
                break
            if req_name and cand_name and (req_name in cand_name or cand_name in req_name):
                selected_candidate = cand
                break

        if selected_candidate is None and len(profile_candidates) == 1:
            selected_candidate = profile_candidates[0]

        if selected_candidate and selected_candidate.get("consulta_id"):
            consulta = _select_consulta(
                db,
                m,
                consulta_id=selected_candidate.get("consulta_id"),
                curp=None,
                nss=selected_candidate.get("nss"),
                nombre=selected_candidate.get("nombre"),
            )

    if consulta is None and len(profile_candidates) == 1 and not consulta_id and not curp:
        unique = profile_candidates[0]
        consulta = _select_consulta(
            db,
            m,
            consulta_id=unique.get("consulta_id"),
            curp=None,
            nss=unique.get("nss"),
            nombre=unique.get("nombre"),
        )

    if consulta is None and not (nss or nombre or q):
        return m.render_template(
            "expediente.html",
            request=request,
            consulta=None,
            protocolo_json="Sin detalles",
            archivos_paciente=[],
            search_q=_safe_text(q),
            search_nss=_safe_text(nss),
            search_nombre=_safe_text(nombre),
            profile_candidates=profile_candidates,
            selected_identity={},
            profile_metrics={},
            hospitalizaciones=[],
            timeline_general=[],
            guardia_resumen=[],
            cirugias_eventos=[],
            labs_series=[],
            notas_medicas=[],
            inpatient_episode_summary=[],
            inpatient_notes_history=[],
            active_inpatient_episode={},
            profile_alertas=[],
            active_hospitalizacion={},
            expediente_enriquecido={},
            estudios_paciente=[],
            protocolo_alertas_aditivas=[],
        )

    if consulta is None:
        return m.render_template(
            "expediente.html",
            request=request,
            consulta=None,
            protocolo_json="Sin detalles",
            archivos_paciente=[],
            search_q=_safe_text(q),
            search_nss=_safe_text(nss),
            search_nombre=_safe_text(nombre),
            profile_candidates=profile_candidates,
            selected_identity={},
            profile_metrics={"error": "Paciente no encontrado con NSS/NOMBRE proporcionado."},
            hospitalizaciones=[],
            timeline_general=[],
            guardia_resumen=[],
            cirugias_eventos=[],
            labs_series=[],
            notas_medicas=[],
            inpatient_episode_summary=[],
            inpatient_notes_history=[],
            active_inpatient_episode={},
            profile_alertas=[],
            active_hospitalizacion={},
            expediente_enriquecido={},
            estudios_paciente=[],
            protocolo_alertas_aditivas=[],
        )

    consulta_nss = _norm_nss(consulta.nss)
    target_nss = _norm_nss(nss) or consulta_nss
    target_name = _norm_name(nombre) or _norm_name(consulta.nombre)

    # Consultas asociadas al perfil.
    consultas_q = db.query(m.ConsultaDB)
    if target_nss:
        consultas_q = consultas_q.filter(_nss_compat_expr(m.ConsultaDB.nss, target_nss))
    else:
        consultas_q = consultas_q.filter(_name_compat_expr(m.ConsultaDB.nombre, target_name))
    consultas = consultas_q.order_by(m.ConsultaDB.id.desc()).all()
    consulta_ids = [int(c.id) for c in consultas if c.id is not None]

    # Hospitalizaciones asociadas.
    hosp_q = db.query(m.HospitalizacionDB).order_by(m.HospitalizacionDB.fecha_ingreso.asc(), m.HospitalizacionDB.id.asc())
    if target_nss:
        hosp_q = hosp_q.filter(_nss_compat_expr(m.HospitalizacionDB.nss, target_nss))
    else:
        hosp_q = hosp_q.filter(_name_compat_expr(m.HospitalizacionDB.nombre_completo, target_name))
    if consulta_ids:
        hosp_q = hosp_q.filter(
            or_(
                m.HospitalizacionDB.consulta_id.in_(consulta_ids),
                _nss_compat_expr(m.HospitalizacionDB.nss, target_nss) if target_nss else False,
            )
        )
    hospitalizaciones = hosp_q.all()

    # Guardias (censo/labs/valoraciones/hallazgos) asociadas.
    guardia_query = select(HOSP_GUARDIA_REGISTROS)
    conditions = []
    if consulta_ids:
        conditions.append(HOSP_GUARDIA_REGISTROS.c.consulta_id.in_(consulta_ids))
    if target_nss:
        conditions.append(_nss_compat_expr(HOSP_GUARDIA_REGISTROS.c.nss, target_nss))
    if target_name:
        conditions.append(_name_compat_expr(HOSP_GUARDIA_REGISTROS.c.nombre, target_name))
    if conditions:
        guardia_query = guardia_query.where(or_(*conditions))
    guardia_rows = db.execute(guardia_query.order_by(HOSP_GUARDIA_REGISTROS.c.fecha.asc(), HOSP_GUARDIA_REGISTROS.c.id.asc())).mappings().all()

    # Censo diario snapshot.
    censo_query = db.query(m.HospitalCensoDiarioDB).order_by(m.HospitalCensoDiarioDB.fecha.asc())
    if hospitalizaciones:
        min_d = min([(h.fecha_ingreso or date.today()) for h in hospitalizaciones])
        max_d = max([(h.fecha_egreso or date.today()) for h in hospitalizaciones])
        censo_query = censo_query.filter(and_(m.HospitalCensoDiarioDB.fecha >= min_d, m.HospitalCensoDiarioDB.fecha <= max_d))
    censo_rows = censo_query.all()

    # Core quirófano.
    quirofano_q = db.query(m.QuirofanoDB)
    if consulta_ids:
        quirofano_q = quirofano_q.filter(m.QuirofanoDB.consulta_id.in_(consulta_ids))
    elif consulta.id:
        quirofano_q = quirofano_q.filter(m.QuirofanoDB.consulta_id == consulta.id)
    quirofano_rows = quirofano_q.order_by(m.QuirofanoDB.fecha_programada.asc(), m.QuirofanoDB.id.asc()).all()

    # Labs core.
    lab_q = db.query(m.LabDB)
    if consulta_ids:
        lab_q = lab_q.filter(m.LabDB.consulta_id.in_(consulta_ids))
    elif consulta.id:
        lab_q = lab_q.filter(m.LabDB.consulta_id == consulta.id)
    lab_rows = lab_q.order_by(m.LabDB.timestamp.asc(), m.LabDB.id.asc()).all()

    # Quirófano base dedicada.
    profile_alertas: List[Dict[str, Any]] = []
    flow_event_rows: List[Any] = []
    sdb = m._new_surgical_session(enable_dual_write=True)
    try:
        surg_q = sdb.query(m.SurgicalProgramacionDB)
        surg_conditions = []
        if consulta_ids:
            surg_conditions.append(m.SurgicalProgramacionDB.consulta_id.in_(consulta_ids))
        if target_nss:
            surg_conditions.append(_nss_compat_expr(m.SurgicalProgramacionDB.nss, target_nss))
        if target_name:
            surg_conditions.append(_name_compat_expr(m.SurgicalProgramacionDB.paciente_nombre, target_name))
        if surg_conditions:
            surg_q = surg_q.filter(or_(*surg_conditions))
        surgical_rows = surg_q.order_by(m.SurgicalProgramacionDB.fecha_programada.asc(), m.SurgicalProgramacionDB.id.asc()).all()
        surgical_ids = [r.id for r in surgical_rows]
        postq_rows = []
        if surgical_ids:
            postq_rows = (
                sdb.query(m.SurgicalPostquirurgicaDB)
                .filter(m.SurgicalPostquirurgicaDB.surgical_programacion_id.in_(surgical_ids))
                .order_by(m.SurgicalPostquirurgicaDB.fecha_realizacion.asc(), m.SurgicalPostquirurgicaDB.id.asc())
                .all()
            )
        flujo_q = sdb.query(m.HechoFlujoQuirurgico)
        flujo_conditions = []
        if consulta_ids:
            flujo_conditions.append(m.HechoFlujoQuirurgico.consulta_id.in_(consulta_ids))
        if target_nss:
            flujo_conditions.append(_nss_compat_expr(m.HechoFlujoQuirurgico.nss, target_nss))
        if flujo_conditions:
            flujo_q = flujo_q.filter(or_(*flujo_conditions))
        flow_event_rows = (
            flujo_q.order_by(m.HechoFlujoQuirurgico.creado_en.asc(), m.HechoFlujoQuirurgico.id.asc())
            .limit(2000)
            .all()
        )
        profile_alertas = list_profile_alertas(
            db,
            sdb,
            consulta_ids=consulta_ids,
            target_nss=target_nss,
            limit=250,
        )
    finally:
        sdb.close()

    active_hospitalizacion_row = get_active_hospitalizacion_for_profile(
        db,
        m,
        consulta_ids=consulta_ids,
        target_nss=target_nss,
        target_name=target_name,
    )
    active_hospitalizacion = None
    if active_hospitalizacion_row is not None:
        active_hospitalizacion = {
            "id": active_hospitalizacion_row.id,
            "consulta_id": active_hospitalizacion_row.consulta_id,
            "cama": _safe_text(active_hospitalizacion_row.cama),
            "servicio": _safe_text(active_hospitalizacion_row.servicio),
            "estado_clinico": _safe_text(active_hospitalizacion_row.estado_clinico),
            "estatus": _safe_text(active_hospitalizacion_row.estatus),
            "ingreso_tipo": _safe_text(active_hospitalizacion_row.ingreso_tipo),
            "urgencia_tipo": _safe_text(active_hospitalizacion_row.urgencia_tipo),
            "fecha_ingreso": active_hospitalizacion_row.fecha_ingreso.isoformat() if active_hospitalizacion_row.fecha_ingreso else "",
            "dias_hospitalizacion": active_hospitalizacion_row.dias_hospitalizacion,
        }

    timeline_payload = _build_timeline(
        m=m,
        consulta_ids=consulta_ids,
        target_nss=target_nss,
        target_name=target_name,
        hospitalizaciones=hospitalizaciones,
        guardia_rows=[dict(r) for r in guardia_rows],
        censo_rows=censo_rows,
        quirofano_rows=quirofano_rows,
        surgical_rows=surgical_rows,
        postq_rows=postq_rows,
        lab_rows=lab_rows,
    )

    protocolo_json = (
        json.dumps(consulta.protocolo_detalles, indent=2, ensure_ascii=False)
        if consulta.protocolo_detalles
        else "Sin detalles"
    )
    archivos_rows = (
        db.query(m.ArchivoPacienteDB)
        .filter(m.ArchivoPacienteDB.consulta_id == consulta.id)
        .order_by(m.ArchivoPacienteDB.fecha_subida.desc(), m.ArchivoPacienteDB.id.desc())
        .all()
    )
    archivos_paciente = [
        {
            "id": row.id,
            "nombre_original": row.nombre_original,
            "extension": row.extension or "",
            "tamano_legible": m._format_size(row.tamano_bytes),
            "fecha_subida": row.fecha_subida.strftime("%Y-%m-%d %H:%M") if row.fecha_subida else "",
        }
        for row in archivos_rows
    ]
    estudios_paciente = fetch_consulta_studies(db, m, consulta_id=int(consulta.id), limit=120)

    # Resúmenes adicionales para perfil clínico.
    guardia_resumen_counter: Dict[str, int] = defaultdict(int)
    for row in guardia_rows:
        guardia_resumen_counter[_safe_text(row.get("dataset")).upper() or "NO_REGISTRADO"] += 1
    guardia_resumen = sorted(guardia_resumen_counter.items(), key=lambda kv: (-kv[1], kv[0]))

    postq_latest_by_programacion: Dict[int, Any] = {}
    for pq in postq_rows:
        sp_id = getattr(pq, "surgical_programacion_id", None)
        if sp_id is None:
            continue
        prev = postq_latest_by_programacion.get(int(sp_id))
        if prev is None:
            postq_latest_by_programacion[int(sp_id)] = pq
            continue
        prev_date = getattr(prev, "fecha_realizacion", None) or date.min
        curr_date = getattr(pq, "fecha_realizacion", None) or date.min
        if curr_date >= prev_date:
            postq_latest_by_programacion[int(sp_id)] = pq

    cirugias_eventos: List[Dict[str, Any]] = []
    for row in surgical_rows:
        postq = postq_latest_by_programacion.get(int(row.id))
        fecha_evento = None
        if postq is not None:
            fecha_evento = getattr(postq, "fecha_realizacion", None)
        if fecha_evento is None:
            fecha_evento = row.fecha_realizacion or row.fecha_programada
        diagnostico_evento = _safe_text(getattr(postq, "diagnostico_postop", None)) if postq is not None else ""
        if not diagnostico_evento:
            diagnostico_evento = _safe_text(getattr(row, "diagnostico_postop", None)) or _safe_text(row.patologia or row.diagnostico_principal)
        procedimiento_evento = _safe_text(getattr(postq, "procedimiento_realizado", None)) if postq is not None else ""
        if not procedimiento_evento:
            procedimiento_evento = _safe_text(getattr(row, "procedimiento_realizado", None)) or _safe_text(row.procedimiento_programado or row.procedimiento)
        cirujano_evento = _safe_text(getattr(postq, "cirujano", None)) if postq is not None else ""
        if not cirujano_evento:
            cirujano_evento = _safe_text(row.cirujano)
        sangrado_evento = None
        if postq is not None and getattr(postq, "sangrado_ml", None) is not None:
            sangrado_evento = getattr(postq, "sangrado_ml", None)
        elif getattr(row, "sangrado_ml", None) is not None:
            sangrado_evento = getattr(row, "sangrado_ml", None)
        hallazgos_evento = ""
        if postq is not None:
            hallazgos_evento = _safe_text(getattr(postq, "complicaciones", None)) or _safe_text(getattr(postq, "nota_postquirurgica", None))
        if not hallazgos_evento:
            hallazgos_evento = _safe_text(getattr(row, "complicaciones_postquirurgicas", None)) or _safe_text(getattr(row, "nota_postquirurgica", None)) or _safe_text(getattr(row, "notas", None))
        cirugias_eventos.append(
            {
                "fecha": fecha_evento.isoformat() if fecha_evento else "",
                "origen": "URGENCIA" if _safe_text(row.modulo_origen).upper() == "QUIROFANO_URGENCIA" else "PROGRAMADA",
                "estatus": _safe_text(row.estatus).upper() or "NO_REGISTRADO",
                "diagnostico": diagnostico_evento,
                "procedimiento": procedimiento_evento,
                "cirujano": cirujano_evento,
                "sangrado_ml": sangrado_evento,
                "hallazgos": hallazgos_evento[:220] if hallazgos_evento else "",
            }
        )

    labs_series: List[Dict[str, Any]] = []
    labs_by_date: Dict[str, Dict[str, float]] = defaultdict(dict)
    for lab in lab_rows:
        if lab.timestamp is None:
            continue
        key = lab.timestamp.date().isoformat()
        payload = {
            "TEST_NAME": _safe_text(lab.test_name).upper(),
            "TEST_CODE": _safe_text(lab.test_code).upper(),
            "VALUE": _safe_text(lab.value),
        }
        markers = _extract_lab_markers_from_payload(payload)
        if markers:
            labs_by_date[key].update(markers)
    for day_key in sorted(labs_by_date.keys()):
        labs_series.append({"fecha": day_key, **labs_by_date[day_key]})

    hospitalizaciones_payload: List[Dict[str, Any]] = []
    for idx, h in enumerate(hospitalizaciones):
        hospitalizaciones_payload.append(
            {
                "id": h.id,
                "tipo": "PRIMERA HOSPITALIZACIÓN" if idx == 0 else f"HOSPITALIZACIÓN PREVIA #{idx}",
                "fecha_ingreso": h.fecha_ingreso.isoformat() if h.fecha_ingreso else "",
                "fecha_egreso": h.fecha_egreso.isoformat() if h.fecha_egreso else "",
                "diagnostico": _safe_text(h.diagnostico),
                "ingreso_tipo": _safe_text(h.ingreso_tipo),
                "urgencia_tipo": _safe_text(h.urgencia_tipo),
                "hgz_envio": _safe_text(h.hgz_envio),
                "estado_clinico": _safe_text(h.estado_clinico),
                "cama": _safe_text(h.cama),
            }
        )

    flujo_clinico = _build_flujo_clinico_view(
        consulta_exists=consulta is not None,
        hospitalizaciones_payload=hospitalizaciones_payload,
        cirugias_eventos=cirugias_eventos,
        flow_event_rows=flow_event_rows,
        active_hospitalizacion=active_hospitalizacion,
    )

    total_hosp = len(hospitalizaciones_payload)
    profile_metrics = {
        "total_consultas": len(consultas),
        "total_hospitalizaciones": total_hosp,
        "primera_hospitalizacion": hospitalizaciones_payload[0]["fecha_ingreso"] if total_hosp else "",
        "ultima_hospitalizacion": hospitalizaciones_payload[-1]["fecha_ingreso"] if total_hosp else "",
        "hospitalizacion_previa": "SI" if total_hosp > 1 else "NO",
        "total_eventos_guardia": len(guardia_rows),
        "total_cirugias": len(cirugias_eventos),
        "total_eventos_flujo": len(flow_event_rows),
        "total_labs_seriados": len(labs_series),
        "estado_flujo_actual": flujo_clinico.get("estado_actual", "CONSULTA"),
    }

    selected_identity = {
        "nss": target_nss,
        "nombre": target_name or _norm_name(consulta.nombre),
        "consulta_id": consulta.id,
    }
    notas_medicas = list_notas_medicas_profile(
        db,
        consulta_ids=consulta_ids,
        target_nss=target_nss,
        target_name=target_name,
        limit=500,
    )
    active_inpatient_episode: Dict[str, Any] = {}
    inpatient_episode_summary: List[Dict[str, Any]] = []
    inpatient_notes_history: List[Dict[str, Any]] = []
    inpatient_tags: List[Dict[str, Any]] = []
    inpatient_soportes: Dict[str, Any] = {
        "drenaje_activo": {},
        "dispositivo_activo": {},
        "drenajes_temporalidad": [],
        "foley_temporalidad": [],
        "indice_urinario_promedio": None,
    }
    if target_nss:
        try:
            active_ep = get_active_episode_by_patient(db, patient_id=target_nss)
            active_inpatient_episode = active_ep or {}
            raw_eps = list_patient_episodes(
                db,
                patient_id=target_nss,
                consulta_ids=consulta_ids,
                limit=800,
            )
            inpatient_episode_summary = summarize_patient_episodes(
                db,
                m,
                patient_id=target_nss,
                consulta_ids=consulta_ids,
                episodes=raw_eps,
            )
            inpatient_notes_history = list_patient_daily_notes(
                db,
                patient_id=target_nss,
                consulta_ids=consulta_ids,
                limit=2500,
            )
            try:
                inpatient_tags = list_inpatient_tags_v2(
                    db,
                    consulta_id=int(consulta.id),
                    hospitalizacion_id=(active_hospitalizacion or {}).get("id"),
                    limit=1000,
                )
            except Exception:
                inpatient_tags = []
            try:
                from app.services.inpatient_devices_events_service import list_devices, list_events

                hosp_id_for_support = (active_hospitalizacion or {}).get("id")
                dev_rows = list_devices(
                    db,
                    consulta_id=int(consulta.id),
                    hospitalizacion_id=hosp_id_for_support,
                    limit=5000,
                )
                drain_types = {"PENROSE", "SARATOGA", "JACKSON", "NEFROSTOMIA", "CONDUCTO ILEAL", "URETEROSTOMA", "DRENAJE PELVICO"}
                support_types = {"SONDA FOLEY", "CATETER JJ", "CATETER URETERAL", "CATETER URETERAL POR REPARACION POR FISTULA VESICOVAGINAL"}
                drain_active = next((d for d in dev_rows if bool(d.get("present")) and _safe_text(d.get("device_type")).upper() in drain_types), {})
                support_active = next((d for d in dev_rows if bool(d.get("present")) and _safe_text(d.get("device_type")).upper() in support_types), {})

                ev_rows = list_events(
                    db,
                    consulta_id=int(consulta.id),
                    hospitalizacion_id=hosp_id_for_support,
                    limit=5000,
                )
                drenajes_temporalidad: List[Dict[str, Any]] = []
                foley_temporalidad: List[Dict[str, Any]] = []
                idx_vals: List[float] = []
                drain_by_day: Dict[str, float] = {}
                foley_by_day: Dict[str, float] = {}
                for ev in ev_rows:
                    et = _safe_text(ev.get("event_type")).upper()
                    payload = ev.get("payload") if isinstance(ev.get("payload"), dict) else {}
                    if et == "DRAIN_OUTPUT_RECORDED":
                        day = _day_iso_from_dt(ev.get("event_time"))
                        ml_val = _to_float(payload.get("output_ml"))
                        if day and ml_val is not None:
                            drain_by_day[day] = float(drain_by_day.get(day, 0.0)) + float(ml_val)
                        drenajes_temporalidad.append(
                            {
                                "fecha": _safe_text(ev.get("event_time")),
                                "tipo": _safe_text(payload.get("drain_type")),
                                "lateralidad": _safe_text(payload.get("side")),
                                "gasto_ml": payload.get("output_ml"),
                            }
                        )
                    elif et == "FOLEY_URESIS_RECORDED":
                        day = _day_iso_from_dt(ev.get("event_time"))
                        u_val = _to_float(payload.get("output_ml"))
                        if day and u_val is not None:
                            foley_by_day[day] = float(foley_by_day.get(day, 0.0)) + float(u_val)
                        idx = payload.get("urinary_index_ml_kg_h")
                        try:
                            if idx is not None:
                                idx_vals.append(float(idx))
                        except Exception:
                            logger.debug("Índice urinario inválido en evento de soporte", exc_info=True)
                        foley_temporalidad.append(
                            {
                                "fecha": _safe_text(ev.get("event_time")),
                                "uresis_ml": payload.get("output_ml"),
                                "indice_urinario": payload.get("urinary_index_ml_kg_h"),
                                "fr": _safe_text(payload.get("foley_fr")),
                            }
                        )
                drain_pct_vals: List[float] = []
                for row in drenajes_temporalidad:
                    day = _day_iso_from_dt(row.get("fecha"))
                    if not day:
                        row["gasto_pct_egreso_dia"] = None
                        continue
                    d_ml = float(drain_by_day.get(day, 0.0))
                    f_ml = float(foley_by_day.get(day, 0.0))
                    total = d_ml + f_ml
                    pct = round((d_ml / total) * 100.0, 2) if total > 0 else None
                    row["gasto_pct_egreso_dia"] = pct
                    if pct is not None:
                        drain_pct_vals.append(float(pct))
                foley_daily_vals = [float(v) for v in foley_by_day.values() if v is not None]
                inpatient_soportes = {
                    "drenaje_activo": drain_active,
                    "dispositivo_activo": support_active,
                    "drenajes_temporalidad": drenajes_temporalidad[:500],
                    "foley_temporalidad": foley_temporalidad[:500],
                    "indice_urinario_promedio": round(sum(idx_vals) / len(idx_vals), 4) if idx_vals else None,
                    "gasto_drenaje_pct_promedio": round(sum(drain_pct_vals) / len(drain_pct_vals), 2) if drain_pct_vals else None,
                    "uresis_foley_promedio_ml": round(sum(foley_daily_vals) / len(foley_daily_vals), 2) if foley_daily_vals else None,
                }
            except Exception:
                inpatient_soportes = {
                    "drenaje_activo": {},
                    "dispositivo_activo": {},
                    "drenajes_temporalidad": [],
                    "foley_temporalidad": [],
                    "indice_urinario_promedio": None,
                    "gasto_drenaje_pct_promedio": None,
                    "uresis_foley_promedio_ml": None,
                }
        except Exception:
            active_inpatient_episode = {}
            inpatient_episode_summary = []
            inpatient_notes_history = []
            inpatient_tags = []
    expediente_enriquecido = get_enriched_by_consulta_id(db, int(consulta.id))
    protocolo_alertas_aditivas: List[str] = []
    try:
        proto_dict = consulta.protocolo_detalles if isinstance(consulta.protocolo_detalles, dict) else {}
        raw_alerts = proto_dict.get("alertas_aditivas", [])
        if isinstance(raw_alerts, list):
            protocolo_alertas_aditivas = [str(x) for x in raw_alerts if str(x).strip()]
    except Exception:
        protocolo_alertas_aditivas = []
    if expediente_enriquecido:
        profile_metrics["completitud_expediente_pct"] = expediente_enriquecido.get("completitud_pct", 0.0)
    profile_metrics["total_notas_medicas"] = len(notas_medicas)
    profile_metrics["total_notas_intrahospitalarias"] = len(inpatient_notes_history)
    profile_metrics["total_episodios_intrahospitalarios"] = len(inpatient_episode_summary)
    profile_metrics["total_tags_clinicos"] = len(inpatient_tags)
    profile_metrics["total_gastos_drenaje"] = len(inpatient_soportes.get("drenajes_temporalidad") or [])
    profile_metrics["total_registros_uresis"] = len(inpatient_soportes.get("foley_temporalidad") or [])
    profile_metrics["episodio_intrahospitalario_activo"] = "SI" if active_inpatient_episode else "NO"
    profile_metrics["total_alertas_perfil"] = len(profile_alertas)
    inconsistencias: List[Dict[str, Any]] = []
    if total_hosp > 0 and not inpatient_notes_history:
        inconsistencias.append(
            {
                "nivel": "ALTA",
                "codigo": "SIN_NOTAS_INTRA",
                "detalle": "Hay hospitalizaciones registradas sin notas intrahospitalarias estructuradas.",
            }
        )
    if total_hosp > 0 and not labs_series:
        inconsistencias.append(
            {
                "nivel": "MEDIA",
                "codigo": "SIN_LABS_SERIADOS",
                "detalle": "No se detectaron laboratorios seriados asociados al perfil.",
            }
        )
    if active_hospitalizacion and not active_inpatient_episode:
        inconsistencias.append(
            {
                "nivel": "MEDIA",
                "codigo": "EPISODIO_NO_SINCRONIZADO",
                "detalle": "Existe hospitalización activa sin episodio estructurado sincronizado.",
            }
        )
    if total_hosp > 0 and len(cirugias_eventos) > 0 and not any(_safe_text(h.get("fecha_egreso")) for h in hospitalizaciones_payload):
        inconsistencias.append(
            {
                "nivel": "MEDIA",
                "codigo": "SIN_EGRESOS_REGISTRADOS",
                "detalle": "Hay eventos quirúrgicos/hospitalarios sin evidencia de egreso capturado.",
            }
        )
    try:
        active_dupes = (
            db.query(m.HospitalizacionDB)
            .filter(m.HospitalizacionDB.consulta_id.in_(consulta_ids))
            .filter(m.HospitalizacionDB.estatus == "ACTIVO")
            .count()
        )
    except Exception:
        active_dupes = 0
    if int(active_dupes or 0) > 1:
        inconsistencias.append(
            {
                "nivel": "ALTA",
                "codigo": "DUPLICIDAD_ACTIVOS",
                "detalle": f"Se detectaron {active_dupes} episodios ACTIVO para el mismo perfil.",
            }
        )
    score_base = 100
    score_base -= len([x for x in inconsistencias if x.get("nivel") == "ALTA"]) * 20
    score_base -= len([x for x in inconsistencias if x.get("nivel") == "MEDIA"]) * 10
    consistencia_pct = max(0, min(100, score_base))
    consistencia_longitudinal = {
        "consistencia_pct": consistencia_pct,
        "inconsistencias": inconsistencias,
        "resumen": {
            "episodios": total_hosp,
            "notas_intrahospitalarias": len(inpatient_notes_history),
            "labs_seriados": len(labs_series),
            "eventos_quirurgicos": len(cirugias_eventos),
            "egresos_registrados": sum(1 for h in hospitalizaciones_payload if _safe_text(h.get("fecha_egreso"))),
            "activos_detectados": int(active_dupes or 0),
        },
    }
    profile_metrics["consistencia_longitudinal_pct"] = consistencia_pct
    log_expediente_access(db, request, consulta_id=int(consulta.id))

    response = m.render_template(
        "expediente.html",
        request=request,
        consulta=consulta,
        protocolo_json=protocolo_json,
        archivos_paciente=archivos_paciente,
        search_q=_safe_text(q),
        search_nss=_safe_text(nss),
        search_nombre=_safe_text(nombre),
        profile_candidates=profile_candidates,
        selected_identity=selected_identity,
        profile_metrics=profile_metrics,
        hospitalizaciones=hospitalizaciones_payload,
        timeline_episodes=timeline_payload.get("episodes", []),
        timeline_general=timeline_payload.get("timeline_general", []),
        guardia_resumen=guardia_resumen,
        cirugias_eventos=cirugias_eventos[:200],
        labs_series=labs_series[:200],
        notas_medicas=notas_medicas,
        inpatient_episode_summary=inpatient_episode_summary[:200],
        inpatient_notes_history=inpatient_notes_history[:1000],
        inpatient_tags=inpatient_tags[:500],
        inpatient_soportes=inpatient_soportes,
        active_inpatient_episode=active_inpatient_episode,
        profile_alertas=profile_alertas[:200],
        active_hospitalizacion=active_hospitalizacion or {},
        flujo_clinico=flujo_clinico,
        consistencia_longitudinal=consistencia_longitudinal,
        expediente_enriquecido=expediente_enriquecido,
        estudios_paciente=estudios_paciente,
        protocolo_alertas_aditivas=protocolo_alertas_aditivas,
    )
    try:
        actor = _safe_text(request.headers.get("X-User") or "ui_shell")
        ctx_payload = save_active_context(
            db,
            actor=actor,
            context={
                "consulta_id": int(getattr(consulta, "id", 0) or 0),
                "hospitalizacion_id": int(active_hospitalizacion.get("id")) if (active_hospitalizacion and active_hospitalizacion.get("id")) else None,
                "nss": selected_identity.get("nss") or _norm_nss(consulta.nss),
                "nombre": selected_identity.get("nombre") or _norm_name(consulta.nombre),
                "source": "expediente_profile",
            },
            source_route="/expediente",
        )
        response.set_cookie(
            key="rnp_patient_context",
            value=quote(json.dumps(ctx_payload, ensure_ascii=False)),
            max_age=60 * 60 * 24 * 14,
            path="/",
            samesite="lax",
            httponly=False,
        )
    except Exception:
        logger.debug("No se pudo persistir contexto activo desde expediente", exc_info=True)
    return response
