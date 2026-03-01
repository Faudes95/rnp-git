from __future__ import annotations

from typing import Any, Dict, Iterable, List, Sequence, Set, Tuple


DEFAULT_REQUIRED_FORM_MAP: Dict[str, str] = {
    "nss": "NSS",
    "agregado_medico": "Agregado médico",
    "nombre_completo": "Nombre completo",
    "edad": "Edad",
    "sexo": "Sexo",
    "patologia": "Patología",
    "procedimiento_programado": "Procedimiento programado",
    "insumos_solicitados": "Insumos solicitados",
    "hgz": "HGZ",
}


def classify_pathology_group(
    patologia: str,
    *,
    onco_set: Set[str],
    litiasis_set: Set[str],
) -> str:
    if patologia in onco_set:
        return "ONCOLOGICO"
    if patologia in litiasis_set:
        return "LITIASIS_URINARIA"
    return "OTRAS_PATOLOGIAS"


def classify_procedure_group(
    procedimiento: str,
    abordaje: str,
    sistema_succion: str,
    *,
    procedimiento_succion: str,
    endoscopicos: Set[str],
    percutaneos: Set[str],
    abiertos: Set[str],
) -> str:
    if procedimiento == procedimiento_succion:
        return "ENDOSCOPICA_CON_SUCCION"
    if procedimiento in endoscopicos:
        return "ENDOSCOPICA"
    if procedimiento in percutaneos:
        return "PERCUTANEA"
    if procedimiento in abiertos:
        return "ABIERTA"
    if abordaje == "ABIERTO":
        return "ABIERTA"
    if abordaje == "LAPAROSCOPICO":
        return "LAPAROSCOPICA"
    if abordaje == "ABIERTO + LAPAROSCOPICO":
        return "ABIERTO_LAPAROSCOPICO"
    if sistema_succion in {"FANS", "DISS"}:
        return "ENDOSCOPICA_CON_SUCCION"
    return "NO_CLASIFICADO"


def is_required_form_complete(
    data: Dict[str, Any],
    required_map: Dict[str, str] | None = None,
) -> Tuple[bool, List[str]]:
    req = required_map or DEFAULT_REQUIRED_FORM_MAP
    missing: List[str] = []
    for key, label in req.items():
        value = data.get(key)
        if value is None:
            missing.append(label)
            continue
        if isinstance(value, str) and value.strip() == "":
            missing.append(label)
    return len(missing) == 0, missing


def enforce_required_fields_model(
    model: Any,
    *,
    skip_fields: Iterable[str],
    required_sentinels: Sequence[str],
) -> None:
    skip = set(skip_fields or [])
    missing: List[str] = []
    for name in type(model).model_fields:
        if name in skip:
            continue
        value = getattr(model, name)
        if value is None:
            missing.append(name)
        elif isinstance(value, str) and value.strip() == "":
            missing.append(name)
    if missing:
        missing_sorted = ", ".join(sorted(missing))
        raise ValueError(
            "Campos obligatorios faltantes. Use valores válidos o "
            f"{', '.join(sorted(required_sentinels))} cuando aplique. "
            f"Faltantes: {missing_sorted}"
        )
