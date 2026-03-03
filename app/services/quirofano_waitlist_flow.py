from __future__ import annotations

from datetime import date
from typing import Any, Dict, List, Optional
from urllib.parse import quote

from fastapi import Request
from fastapi.responses import HTMLResponse, RedirectResponse
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

from app.core.app_context import main_proxy as m
from app.core.time_utils import utcnow
import sqlite3
from pathlib import Path


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _safe_int(value: Any) -> Optional[int]:
    try:
        text = _safe_text(value)
        if not text:
            return None
        return int(float(text))
    except Exception:
        return None


def _priority_rank(value: str) -> int:
    table = {
        "MUY ALTA": 0,
        "ALTA": 1,
        "MEDIA": 2,
        "BAJA": 3,
    }
    return table.get(_safe_text(value).upper(), 9)


def _waitlist_db_path() -> Path:
    # BD dedicada solicitada para lista de pacientes en espera de programación quirúrgica.
    return Path(__file__).resolve().parents[2] / "lista_pacientes_espera_programacion_quirurgica.db"


def _ensure_waitlist_mirror_schema(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS waitlist_entries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            surgical_programacion_id INTEGER UNIQUE,
            consulta_id INTEGER,
            nss TEXT,
            paciente_nombre TEXT,
            edad INTEGER,
            sexo TEXT,
            patologia TEXT,
            ecog TEXT,
            hgz TEXT,
            procedimiento_programado TEXT,
            fecha_ingreso TEXT,
            prioridad_clinica TEXT,
            motivo_prioridad TEXT,
            estatus TEXT,
            dias_en_espera INTEGER,
            source_module TEXT,
            updated_at TEXT
        )
        """
    )
    conn.commit()


def _upsert_waitlist_mirror(
    *,
    surgical_programacion_id: int,
    consulta_id: int,
    nss: str,
    paciente_nombre: str,
    edad: int,
    sexo: str,
    patologia: str,
    ecog: str,
    hgz: str,
    procedimiento_programado: str,
    fecha_ingreso: str,
    prioridad_clinica: str,
    motivo_prioridad: str,
    estatus: str,
    dias_en_espera: int,
) -> None:
    db_file = _waitlist_db_path()
    db_file.parent.mkdir(parents=True, exist_ok=True)
    now_iso = utcnow().isoformat(timespec="seconds")
    with sqlite3.connect(str(db_file)) as conn:
        _ensure_waitlist_mirror_schema(conn)
        conn.execute(
            """
            INSERT INTO waitlist_entries(
                surgical_programacion_id, consulta_id, nss, paciente_nombre, edad, sexo,
                patologia, ecog, hgz, procedimiento_programado, fecha_ingreso,
                prioridad_clinica, motivo_prioridad, estatus, dias_en_espera,
                source_module, updated_at
            )
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(surgical_programacion_id) DO UPDATE SET
                consulta_id=excluded.consulta_id,
                nss=excluded.nss,
                paciente_nombre=excluded.paciente_nombre,
                edad=excluded.edad,
                sexo=excluded.sexo,
                patologia=excluded.patologia,
                ecog=excluded.ecog,
                hgz=excluded.hgz,
                procedimiento_programado=excluded.procedimiento_programado,
                fecha_ingreso=excluded.fecha_ingreso,
                prioridad_clinica=excluded.prioridad_clinica,
                motivo_prioridad=excluded.motivo_prioridad,
                estatus=excluded.estatus,
                dias_en_espera=excluded.dias_en_espera,
                source_module=excluded.source_module,
                updated_at=excluded.updated_at
            """,
            (
                int(surgical_programacion_id),
                int(consulta_id),
                _safe_text(nss),
                _safe_text(paciente_nombre),
                int(edad),
                _safe_text(sexo),
                _safe_text(patologia),
                _safe_text(ecog),
                _safe_text(hgz),
                _safe_text(procedimiento_programado),
                _safe_text(fecha_ingreso),
                _safe_text(prioridad_clinica),
                _safe_text(motivo_prioridad),
                _safe_text(estatus),
                int(dias_en_espera),
                "LISTA_ESPERA_QX",
                now_iso,
            ),
        )
        conn.commit()


def _build_wait_days(row: Any) -> int:
    dias = _safe_int(getattr(row, "dias_en_espera", None))
    if dias is not None and dias >= 0:
        return int(dias)
    base_dt = getattr(row, "fecha_ingreso_pendiente_programar", None)
    if base_dt is None:
        return 0
    try:
        return max((date.today() - base_dt.date()).days, 0)
    except Exception:
        return 0


def _is_onco(patologia: str) -> bool:
    return _safe_text(patologia).upper() in {str(x).upper() for x in (m.QUIROFANO_PATOLOGIAS_ONCOLOGICAS or [])}


def _waitlist_base_query(sdb: Session):
    return (
        sdb.query(m.SurgicalProgramacionDB)
        .filter(
            or_(
                m.SurgicalProgramacionDB.pendiente_programar == "SI",
                m.SurgicalProgramacionDB.estatus == "LISTA_ESPERA",
            )
        )
    )


def _next_virtual_quirofano_id(sdb: Session) -> int:
    min_val = sdb.query(func.min(m.SurgicalProgramacionDB.quirofano_id)).scalar()
    if min_val is None:
        return -1
    try:
        min_int = int(min_val)
    except Exception:
        return -1
    return -1 if min_int >= 0 else (min_int - 1)


def _find_consulta(db: Session, consulta_id: Optional[int], nss: str) -> Optional[Any]:
    if consulta_id is not None:
        row = db.query(m.ConsultaDB).filter(m.ConsultaDB.id == int(consulta_id)).first()
        if row is not None:
            return row
    nss_norm = m.normalize_nss(nss)
    if nss_norm:
        return (
            db.query(m.ConsultaDB)
            .filter(m.ConsultaDB.nss == nss_norm)
            .order_by(m.ConsultaDB.id.desc())
            .first()
        )
    return None


def _latest_programacion_for_prefill(sdb: Session, *, consulta_id: Optional[int], nss: str) -> Optional[Any]:
    q = sdb.query(m.SurgicalProgramacionDB)
    filters = []
    if consulta_id is not None:
        filters.append(m.SurgicalProgramacionDB.consulta_id == int(consulta_id))
    nss_norm = m.normalize_nss(nss)
    if nss_norm:
        filters.append(m.SurgicalProgramacionDB.nss == nss_norm)
    if filters:
        q = q.filter(or_(*filters))
    return q.order_by(m.SurgicalProgramacionDB.id.desc()).first()


def _build_prefill(consulta: Optional[Any], prev: Optional[Any]) -> Dict[str, Any]:
    patologia = _safe_text(getattr(prev, "patologia", "")) or _safe_text(getattr(consulta, "diagnostico_principal", ""))
    sexo = _safe_text(getattr(prev, "sexo", "")) or _safe_text(getattr(consulta, "sexo", ""))
    nss = m.normalize_nss(_safe_text(getattr(consulta, "nss", "")) or _safe_text(getattr(prev, "nss", "")))
    edad = _safe_int(getattr(prev, "edad", None))
    if edad is None:
        edad = _safe_int(getattr(consulta, "edad", None))
    prioridad_default = "ALTA" if _is_onco(patologia) else "MEDIA"

    consulta_hgz = (
        _safe_text(getattr(consulta, "hgz_envio", ""))
        or _safe_text(getattr(consulta, "hgz", ""))
    )
    consulta_ecog = (
        _safe_text(getattr(consulta, "rinon_ecog", ""))
        or _safe_text(getattr(consulta, "vejiga_ecog", ""))
        or _safe_text(getattr(consulta, "pros_ecog", ""))
    )

    return {
        "consulta_id": _safe_int(getattr(consulta, "id", None)) or _safe_int(getattr(prev, "consulta_id", None)),
        "nss": nss,
        "nombre": _safe_text(getattr(prev, "paciente_nombre", "")) or _safe_text(getattr(consulta, "nombre", "")),
        "edad": edad,
        "sexo": sexo,
        "patologia": patologia,
        "hgz": _safe_text(getattr(prev, "hgz", "")) or consulta_hgz,
        "agregado_medico": _safe_text(getattr(prev, "agregado_medico", "")) or _safe_text(getattr(consulta, "agregado_medico", "")),
        "ecog": _safe_text(getattr(prev, "ecog", "")) or consulta_ecog,
        "tnm": _safe_text(getattr(prev, "tnm", "")),
        "charlson": _safe_text(getattr(prev, "charlson", "")),
        "etapa_clinica": _safe_text(getattr(prev, "etapa_clinica", "")),
        "ipss": _safe_text(getattr(prev, "ipss", "")),
        "gleason": _safe_text(getattr(prev, "gleason", "")),
        "ape": _safe_text(getattr(prev, "ape", "")),
        "procedimiento_programado": _safe_text(getattr(prev, "procedimiento_programado", "")),
        "prioridad_clinica": _safe_text(getattr(prev, "prioridad_clinica", "")) or prioridad_default,
        "motivo_prioridad": _safe_text(getattr(prev, "motivo_prioridad", "")),
        "notas": _safe_text(getattr(prev, "notas", "")),
        "litiasis_tamano_rango": _safe_text(getattr(prev, "litiasis_tamano_rango", "")),
        "litiasis_subtipo_20": _safe_text(getattr(prev, "litiasis_subtipo_20", "")),
        "litiasis_ubicacion": _safe_text(getattr(prev, "litiasis_ubicacion", "")),
        "litiasis_ubicacion_multiple": _safe_text(getattr(prev, "litiasis_ubicacion_multiple", "")),
        "uh_rango": _safe_text(getattr(prev, "uh_rango", "")),
        "hidronefrosis": _safe_text(getattr(prev, "hidronefrosis", "")),
        "tacto_rectal": _safe_text(getattr(prev, "tacto_rectal", "")),
        "historial_ape": _safe_text(getattr(prev, "historial_ape", "")),
    }


def _render_ingreso(
    request: Request,
    *,
    prefill: Dict[str, Any],
    saved: str = "",
    error: str = "",
) -> HTMLResponse:
    return m.render_template(
        "quirofano_lista_espera_ingreso.html",
        request=request,
        prefill=prefill,
        saved=_safe_text(saved),
        error=_safe_text(error),
        sexo_options=m.QUIROFANO_SEXOS,
        patologia_options=m.QUIROFANO_PATOLOGIAS,
        procedimiento_options=m.QUIROFANO_PROCEDIMIENTOS,
        patologias_onco=sorted(m.QUIROFANO_PATOLOGIAS_ONCOLOGICAS),
        patologias_litiasis=sorted(m.QUIROFANO_PATOLOGIAS_LITIASIS),
        procedimiento_abiertos=sorted(m.QUIROFANO_PROCEDIMIENTOS_ABIERTOS),
    )


async def render_waitlist_ingreso_flow(
    request: Request,
    db: Session,
    sdb: Session,
    *,
    consulta_id: Optional[int] = None,
    nss: Optional[str] = None,
    saved: str = "",
    error: str = "",
) -> HTMLResponse:
    consulta = _find_consulta(db, consulta_id, _safe_text(nss))
    prefill_nss = _safe_text(nss) or _safe_text(getattr(consulta, "nss", ""))
    prev = _latest_programacion_for_prefill(
        sdb,
        consulta_id=_safe_int(getattr(consulta, "id", None)) if consulta is not None else consulta_id,
        nss=prefill_nss,
    )
    prefill = _build_prefill(consulta, prev)
    return _render_ingreso(request, prefill=prefill, saved=saved, error=error)


async def save_waitlist_ingreso_flow(request: Request, db: Session, sdb: Session) -> HTMLResponse:
    form = await request.form()
    data = {k: v for k, v in form.items()}
    m.validate_csrf(data, request)

    consulta_id = _safe_int(data.get("consulta_id"))
    consulta = _find_consulta(db, consulta_id, _safe_text(data.get("nss")))
    if consulta is None:
        return RedirectResponse(url="/quirofano/lista-espera/ingresar?error=Consulta%20no%20encontrada", status_code=303)

    nss = m.normalize_nss(_safe_text(data.get("nss")) or _safe_text(getattr(consulta, "nss", "")))
    nombre = _safe_text(data.get("nombre")) or _safe_text(getattr(consulta, "nombre", ""))
    edad = _safe_int(data.get("edad"))
    sexo = m.normalize_upper(data.get("sexo"))
    patologia = m.normalize_upper(data.get("patologia"))
    procedimiento = m.normalize_upper(data.get("procedimiento_programado"))
    hgz = _safe_text(data.get("hgz")).upper()
    prioridad = _safe_text(data.get("prioridad_clinica")).upper() or ("ALTA" if _is_onco(patologia) else "MEDIA")

    if len(nss) != 10:
        return RedirectResponse(
            url=f"/quirofano/lista-espera/ingresar?consulta_id={int(consulta.id)}&error=NSS%20invalido",
            status_code=303,
        )
    if not nombre:
        return RedirectResponse(
            url=f"/quirofano/lista-espera/ingresar?consulta_id={int(consulta.id)}&error=Nombre%20obligatorio",
            status_code=303,
        )
    if edad is None or edad < 0:
        return RedirectResponse(
            url=f"/quirofano/lista-espera/ingresar?consulta_id={int(consulta.id)}&error=Edad%20invalida",
            status_code=303,
        )
    if sexo not in set(m.QUIROFANO_SEXOS):
        return RedirectResponse(
            url=f"/quirofano/lista-espera/ingresar?consulta_id={int(consulta.id)}&error=Sexo%20invalido",
            status_code=303,
        )
    if patologia not in set(m.QUIROFANO_PATOLOGIAS):
        return RedirectResponse(
            url=f"/quirofano/lista-espera/ingresar?consulta_id={int(consulta.id)}&error=Diagnostico%20invalido",
            status_code=303,
        )
    if procedimiento not in set(m.QUIROFANO_PROCEDIMIENTOS):
        return RedirectResponse(
            url=f"/quirofano/lista-espera/ingresar?consulta_id={int(consulta.id)}&error=Procedimiento%20invalido",
            status_code=303,
        )
    if not hgz:
        return RedirectResponse(
            url=f"/quirofano/lista-espera/ingresar?consulta_id={int(consulta.id)}&error=HGZ%20obligatorio",
            status_code=303,
        )

    tnm = _safe_text(data.get("tnm")).upper()
    ecog = _safe_text(data.get("ecog")).upper()
    charlson = _safe_text(data.get("charlson")).upper()
    etapa_clinica = _safe_text(data.get("etapa_clinica")).upper()
    ipss = _safe_text(data.get("ipss")).upper()
    gleason = _safe_text(data.get("gleason")).upper()
    ape = _safe_text(data.get("ape")).upper()
    tacto_rectal = _safe_text(data.get("tacto_rectal")).upper()
    historial_ape = _safe_text(data.get("historial_ape")).upper()
    uh_rango = _safe_text(data.get("uh_rango")).upper()
    litiasis_tamano_rango = _safe_text(data.get("litiasis_tamano_rango")).upper()
    litiasis_subtipo_20 = _safe_text(data.get("litiasis_subtipo_20")).upper()
    litiasis_ubicacion = _safe_text(data.get("litiasis_ubicacion")).upper()
    litiasis_ubicacion_multiple = _safe_text(data.get("litiasis_ubicacion_multiple")).upper()
    hidronefrosis = _safe_text(data.get("hidronefrosis")).upper()
    motivo_prioridad = _safe_text(data.get("motivo_prioridad"))
    notas = _safe_text(data.get("notas"))
    agregado_medico = _safe_text(data.get("agregado_medico")) or _safe_text(getattr(consulta, "agregado_medico", ""))

    if _is_onco(patologia) and not ecog:
        return RedirectResponse(
            url=f"/quirofano/lista-espera/ingresar?consulta_id={int(consulta.id)}&error=Para%20patologia%20oncologica%20capture%20ECOG",
            status_code=303,
        )
    if patologia == "CALCULO DEL RIÑON":
        if not litiasis_tamano_rango or not litiasis_ubicacion:
            return RedirectResponse(
                url=f"/quirofano/lista-espera/ingresar?consulta_id={int(consulta.id)}&error=Capture%20tamano%20y%20ubicacion%20de%20litiasis",
                status_code=303,
            )
        if litiasis_tamano_rango == "> 20 MM" and not litiasis_subtipo_20:
            return RedirectResponse(
                url=f"/quirofano/lista-espera/ingresar?consulta_id={int(consulta.id)}&error=Capture%20subtipo%20para%20litiasis%20%3E20mm",
                status_code=303,
            )

    existing = (
        _waitlist_base_query(sdb)
        .filter(
            and_(
                m.SurgicalProgramacionDB.consulta_id == int(consulta.id),
                m.SurgicalProgramacionDB.procedimiento_programado == procedimiento,
            )
        )
        .order_by(m.SurgicalProgramacionDB.id.desc())
        .first()
    )

    base_wait_dt = getattr(existing, "fecha_ingreso_pendiente_programar", None) if existing is not None else utcnow()
    if base_wait_dt is None:
        base_wait_dt = utcnow()
    dias_espera = max((date.today() - base_wait_dt.date()).days, 0)
    grupo_patologia = m.classify_pathology_group(patologia)
    grupo_procedimiento = m.classify_procedure_group(procedimiento, "", "")
    protocol_complete = "SI" if _safe_text(getattr(consulta, "estatus_protocolo", "")).lower() == "completo" else "NO"

    if existing is None:
        row = m.SurgicalProgramacionDB(
            quirofano_id=_next_virtual_quirofano_id(sdb),
            consulta_id=int(consulta.id),
            curp=_safe_text(getattr(consulta, "curp", "")) or None,
            nss=nss,
            agregado_medico=agregado_medico,
            paciente_nombre=nombre.upper(),
            edad=int(edad),
            edad_grupo=m.classify_age_group(int(edad)),
            sexo=sexo,
            grupo_sexo=sexo,
            diagnostico_principal=patologia,
            patologia=patologia,
            grupo_patologia=grupo_patologia,
            procedimiento=procedimiento,
            procedimiento_programado=procedimiento,
            grupo_procedimiento=grupo_procedimiento,
            hgz=hgz,
            tnm=tnm or None,
            ecog=ecog or None,
            charlson=charlson or None,
            etapa_clinica=etapa_clinica or None,
            ipss=ipss or None,
            gleason=gleason or None,
            ape=ape or None,
            tacto_rectal=tacto_rectal or None,
            historial_ape=historial_ape or None,
            uh_rango=uh_rango or None,
            litiasis_tamano_rango=litiasis_tamano_rango or None,
            litiasis_subtipo_20=litiasis_subtipo_20 or None,
            litiasis_ubicacion=litiasis_ubicacion or None,
            litiasis_ubicacion_multiple=litiasis_ubicacion_multiple or None,
            hidronefrosis=hidronefrosis or None,
            estatus="LISTA_ESPERA",
            protocolo_completo=protocol_complete,
            pendiente_programar="SI",
            fecha_ingreso_pendiente_programar=base_wait_dt,
            dias_en_espera=dias_espera,
            prioridad_clinica=prioridad,
            motivo_prioridad=motivo_prioridad or None,
            notas=notas or None,
            modulo_origen="LISTA_ESPERA_QX",
        )
        sdb.add(row)
        action = "created"
    else:
        row = existing
        row.curp = _safe_text(getattr(consulta, "curp", "")) or row.curp
        row.nss = nss
        row.agregado_medico = agregado_medico
        row.paciente_nombre = nombre.upper()
        row.edad = int(edad)
        row.edad_grupo = m.classify_age_group(int(edad))
        row.sexo = sexo
        row.grupo_sexo = sexo
        row.diagnostico_principal = patologia
        row.patologia = patologia
        row.grupo_patologia = grupo_patologia
        row.procedimiento = procedimiento
        row.procedimiento_programado = procedimiento
        row.grupo_procedimiento = grupo_procedimiento
        row.hgz = hgz
        row.tnm = tnm or None
        row.ecog = ecog or None
        row.charlson = charlson or None
        row.etapa_clinica = etapa_clinica or None
        row.ipss = ipss or None
        row.gleason = gleason or None
        row.ape = ape or None
        row.tacto_rectal = tacto_rectal or None
        row.historial_ape = historial_ape or None
        row.uh_rango = uh_rango or None
        row.litiasis_tamano_rango = litiasis_tamano_rango or None
        row.litiasis_subtipo_20 = litiasis_subtipo_20 or None
        row.litiasis_ubicacion = litiasis_ubicacion or None
        row.litiasis_ubicacion_multiple = litiasis_ubicacion_multiple or None
        row.hidronefrosis = hidronefrosis or None
        row.estatus = "LISTA_ESPERA"
        row.protocolo_completo = protocol_complete
        row.pendiente_programar = "SI"
        row.fecha_ingreso_pendiente_programar = base_wait_dt
        row.dias_en_espera = dias_espera
        row.prioridad_clinica = prioridad
        row.motivo_prioridad = motivo_prioridad or None
        row.notas = notas or None
        row.modulo_origen = "LISTA_ESPERA_QX"
        action = "updated"

    sdb.commit()
    entry_id = int(getattr(row, "id", 0) or 0)
    try:
        _upsert_waitlist_mirror(
            surgical_programacion_id=entry_id,
            consulta_id=int(getattr(consulta, "id", 0) or 0),
            nss=nss,
            paciente_nombre=nombre.upper(),
            edad=int(edad),
            sexo=sexo,
            patologia=patologia,
            ecog=ecog,
            hgz=hgz,
            procedimiento_programado=procedimiento,
            fecha_ingreso=base_wait_dt.date().isoformat(),
            prioridad_clinica=prioridad,
            motivo_prioridad=motivo_prioridad,
            estatus="LISTA_ESPERA",
            dias_en_espera=dias_espera,
        )
    except Exception:
        # Falla de mirror no bloquea el flujo clínico principal.
        pass
    query = (
        f"/quirofano/lista-espera/ingresar?consulta_id={int(consulta.id)}"
        f"&saved={quote(action)}&entry_id={entry_id}"
    )
    return RedirectResponse(url=query, status_code=303)


async def render_waitlist_lista_flow(
    request: Request,
    sdb: Session,
    *,
    q: Optional[str] = None,
    prioridad: Optional[str] = None,
    patologia: Optional[str] = None,
    hgz: Optional[str] = None,
) -> HTMLResponse:
    q_txt = _safe_text(q).upper()
    prioridad_txt = _safe_text(prioridad).upper()
    patologia_txt = _safe_text(patologia).upper()
    hgz_txt = _safe_text(hgz).upper()

    query = _waitlist_base_query(sdb)
    if prioridad_txt:
        query = query.filter(m.SurgicalProgramacionDB.prioridad_clinica == prioridad_txt)
    if patologia_txt:
        query = query.filter(m.SurgicalProgramacionDB.patologia == patologia_txt)
    if hgz_txt:
        query = query.filter(m.SurgicalProgramacionDB.hgz == hgz_txt)
    if q_txt:
        query = query.filter(
            or_(
                func.upper(m.SurgicalProgramacionDB.nss).contains(q_txt),
                func.upper(m.SurgicalProgramacionDB.paciente_nombre).contains(q_txt),
                func.upper(m.SurgicalProgramacionDB.patologia).contains(q_txt),
                func.upper(m.SurgicalProgramacionDB.procedimiento_programado).contains(q_txt),
            )
        )

    rows = query.order_by(
        m.SurgicalProgramacionDB.fecha_ingreso_pendiente_programar.asc(),
        m.SurgicalProgramacionDB.id.asc(),
    ).limit(2000).all()

    table: List[Dict[str, Any]] = []
    for row in rows:
        dias = _build_wait_days(row)
        table.append(
            {
                "id": int(row.id),
                "consulta_id": int(row.consulta_id) if row.consulta_id is not None else None,
                "nss": _safe_text(row.nss),
                "paciente_nombre": _safe_text(row.paciente_nombre),
                "edad": row.edad,
                "sexo": _safe_text(row.sexo),
                "patologia": _safe_text(row.patologia),
                "ecog": _safe_text(row.ecog),
                "hgz": _safe_text(row.hgz),
                "procedimiento_programado": _safe_text(row.procedimiento_programado),
                "fecha_ingreso": row.fecha_ingreso_pendiente_programar.date().isoformat()
                if row.fecha_ingreso_pendiente_programar
                else "N/E",
                "dias_espera": dias,
                "prioridad_clinica": _safe_text(row.prioridad_clinica) or "N/E",
                "prioridad_css": (_safe_text(row.prioridad_clinica) or "NE").upper().replace(" ", "-"),
                "motivo_prioridad": _safe_text(row.motivo_prioridad),
                "estatus": _safe_text(row.estatus),
                "onco": _is_onco(_safe_text(row.patologia)),
            }
        )

    table.sort(
        key=lambda r: (
            _priority_rank(r.get("prioridad_clinica") or ""),
            -(r.get("dias_espera") or 0),
            0 if r.get("onco") else 1,
            r.get("fecha_ingreso") or "",
            r.get("id") or 0,
        )
    )

    total = len(table)
    total_onco = sum(1 for r in table if r.get("onco"))
    total_alta = sum(1 for r in table if _safe_text(r.get("prioridad_clinica")).upper() in {"MUY ALTA", "ALTA"})
    promedio_espera = round(sum((r.get("dias_espera") or 0) for r in table) / max(1, total), 1) if total else 0

    return m.render_template(
        "quirofano_lista_espera_lista.html",
        request=request,
        waitlist_rows=table,
        q=q or "",
        prioridad=prioridad_txt,
        patologia=patologia_txt,
        hgz=hgz_txt,
        total=total,
        total_onco=total_onco,
        total_alta=total_alta,
        promedio_espera=promedio_espera,
        waitlist_db_file=str(_waitlist_db_path()),
        patologia_options=m.QUIROFANO_PATOLOGIAS,
    )
