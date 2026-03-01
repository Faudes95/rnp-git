from __future__ import annotations

from datetime import date
from time import sleep
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session


_DYNAMIC_SYMBOLS = [
    "DimFecha",
    "HechoProgramacionQuirurgica",
    "func",
    "Prophet",
    "pd",
    "LinearRegression",
    "np",
    "ensure_dim_paciente_geo_schema",
    "DimPaciente",
    "SurgicalProgramacionDB",
    "_build_patient_hash",
    "ConsultaDB",
    "normalize_curp",
    "geocodificar_direccion",
]


def _ensure_symbols() -> None:
    from app.core.app_context import main_proxy as m

    module_globals = globals()
    missing: List[str] = []
    for symbol in _DYNAMIC_SYMBOLS:
        if symbol in module_globals:
            continue
        try:
            module_globals[symbol] = getattr(m, symbol)
        except Exception:
            missing.append(symbol)
    if missing:
        raise RuntimeError(f"No fue posible resolver símbolos legacy: {', '.join(sorted(missing))}")


def forecast_surgery_payload(dias: int, sdb: Session) -> Tuple[int, Any]:
    _ensure_symbols()
    query = sdb.query(
        DimFecha.fecha.label("fecha"),
        func.count(HechoProgramacionQuirurgica.id).label("y"),
    ).join(
        HechoProgramacionQuirurgica, HechoProgramacionQuirurgica.fecha_id == DimFecha.id
    ).filter(
        HechoProgramacionQuirurgica.estatus == "PROGRAMADA"
    ).group_by(
        DimFecha.fecha
    ).order_by(
        DimFecha.fecha
    )
    rows = query.all()
    if not rows:
        return 400, {"error": "No hay datos históricos"}
    if Prophet is not None and pd is not None:
        df = pd.DataFrame([{"ds": r.fecha, "y": int(r.y)} for r in rows])
        model = Prophet()
        model.fit(df)
        future = model.make_future_dataframe(periods=max(dias, 1))
        forecast = model.predict(future)
        last_ds = df["ds"].max()
        f = forecast[forecast["ds"] > last_ds][["ds", "yhat", "yhat_lower", "yhat_upper"]]
        out = []
        for _, row in f.iterrows():
            out.append(
                {
                    "fecha": row["ds"].date().isoformat() if hasattr(row["ds"], "date") else str(row["ds"]),
                    "yhat": float(row["yhat"]),
                    "yhat_lower": float(row["yhat_lower"]),
                    "yhat_upper": float(row["yhat_upper"]),
                }
            )
        return 200, out

    if LinearRegression is None or np is None:
        return 400, {"error": "Dependencias de pronóstico no disponibles"}
    fechas = [r.fecha for r in rows]
    y = [int(r.y) for r in rows]
    x = np.array([f.toordinal() for f in fechas]).reshape(-1, 1)
    model = LinearRegression()
    model.fit(x, y)
    last_ord = fechas[-1].toordinal()
    result = []
    for i in range(1, max(dias, 1) + 1):
        d = date.fromordinal(last_ord + i)
        pred = float(model.predict(np.array([[d.toordinal()]])).ravel()[0])
        result.append({"fecha": d.isoformat(), "yhat": max(pred, 0.0)})
    return 200, result


def geocodificar_pacientes_pendientes(
    sdb: Session,
    db: Session,
    limite: int = 100,
    sleep_seconds: float = 1.0,
) -> int:
    """
    Geocodifica pacientes en DimPaciente sin coordenadas.
    Usa SurgicalProgramacionDB para identificar paciente y ConsultaDB para dirección.
    """
    _ensure_symbols()
    ensure_dim_paciente_geo_schema()
    pending = sdb.query(DimPaciente).filter(
        (DimPaciente.lat.is_(None)) | (DimPaciente.lon.is_(None))
    ).limit(max(1, int(limite))).all()
    if not pending:
        return 0

    pending_map = {p.paciente_hash: p for p in pending if p.paciente_hash}
    if not pending_map:
        return 0

    programaciones = sdb.query(SurgicalProgramacionDB).order_by(
        SurgicalProgramacionDB.fecha_programada.desc(),
        SurgicalProgramacionDB.id.desc(),
    ).all()
    hash_to_prog: Dict[str, Any] = {}
    for prog in programaciones:
        h = _build_patient_hash(prog.nss, prog.curp, prog.consulta_id)
        if not h:
            continue
        if h in pending_map and h not in hash_to_prog:
            hash_to_prog[h] = prog

    actualizados = 0
    for p_hash, dim in pending_map.items():
        prog = hash_to_prog.get(p_hash)
        if not prog:
            continue

        consulta = None
        if prog.consulta_id:
            consulta = db.query(ConsultaDB).filter(ConsultaDB.id == prog.consulta_id).first()
        if consulta is None and prog.curp:
            consulta = db.query(ConsultaDB).filter(
                ConsultaDB.curp == normalize_curp(prog.curp)
            ).order_by(ConsultaDB.id.desc()).first()

        if not consulta:
            continue

        alcaldia = (consulta.alcaldia or "").strip()
        colonia = (consulta.colonia or "").strip()
        cp = (consulta.cp or "").strip()

        lat, lon = geocodificar_direccion(alcaldia, colonia, cp)
        if lat is None or lon is None:
            continue

        dim.alcaldia = alcaldia or dim.alcaldia
        dim.colonia = colonia or dim.colonia
        dim.cp = cp or dim.cp
        dim.lat = lat
        dim.lon = lon
        actualizados += 1

        if sleep_seconds > 0:
            sleep(sleep_seconds)

    sdb.commit()
    return actualizados


def build_geojson_pacientes_programados(
    sdb: Session,
    limit: int = 2000,
) -> Dict[str, Any]:
    _ensure_symbols()
    safe_limit = max(1, min(int(limit), 5000))

    rows = sdb.query(SurgicalProgramacionDB).filter(
        SurgicalProgramacionDB.estatus == "PROGRAMADA"
    ).order_by(
        SurgicalProgramacionDB.fecha_programada.desc(),
        SurgicalProgramacionDB.id.desc(),
    ).limit(safe_limit).all()

    needed_hashes = set()
    for r in rows:
        h = _build_patient_hash(r.nss, r.curp, r.consulta_id)
        if h:
            needed_hashes.add(h)

    if not needed_hashes:
        return {"type": "FeatureCollection", "features": []}

    dim_rows = sdb.query(DimPaciente).filter(
        DimPaciente.paciente_hash.in_(list(needed_hashes)),
        DimPaciente.lat.isnot(None),
        DimPaciente.lon.isnot(None),
    ).all()
    dim_map = {d.paciente_hash: d for d in dim_rows}

    features: List[Dict[str, Any]] = []
    for r in rows:
        h = _build_patient_hash(r.nss, r.curp, r.consulta_id)
        if not h:
            continue
        dim = dim_map.get(h)
        if not dim:
            continue
        features.append(
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [float(dim.lon), float(dim.lat)]},
                "properties": {
                    "hgz": r.hgz,
                    "patologia": r.patologia,
                    "edad": r.edad,
                    "sexo": r.sexo,
                },
            }
        )
    return {"type": "FeatureCollection", "features": features}

