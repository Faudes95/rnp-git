from __future__ import annotations

import re
from typing import List, Optional, Tuple


def extract_numeric_level(raw_value: Optional[str]) -> int:
    if not raw_value:
        return 0
    match = re.search(r"(\d+)", str(raw_value))
    if not match:
        return 0
    try:
        return int(match.group(1))
    except Exception:
        return 0


def waiting_days_bucket(days_value: Optional[int]) -> str:
    if days_value is None:
        return "NO_REGISTRADO"
    if days_value <= 7:
        return "0-7 DIAS"
    if days_value <= 14:
        return "8-14 DIAS"
    if days_value <= 30:
        return "15-30 DIAS"
    if days_value <= 60:
        return "31-60 DIAS"
    return "MAYOR A 60 DIAS"


def estimate_cancelation_risk(
    *,
    edad: Optional[int],
    ecog: Optional[str],
    charlson: Optional[str],
    dias_espera: Optional[int],
    requiere_intermed: Optional[str],
) -> float:
    """Estimador ligero aditivo (fallback cuando no hay modelo ML operativo)."""
    score = 0.04
    if edad is not None:
        if edad >= 80:
            score += 0.20
        elif edad >= 70:
            score += 0.14
        elif edad >= 60:
            score += 0.08
    ecog_level = extract_numeric_level(ecog)
    charlson_level = extract_numeric_level(charlson)
    score += min(ecog_level, 4) * 0.08
    score += min(charlson_level, 8) * 0.03
    if dias_espera is not None:
        if dias_espera > 60:
            score += 0.20
        elif dias_espera > 30:
            score += 0.12
        elif dias_espera > 14:
            score += 0.06
    if (requiere_intermed or "").strip().upper() == "SI":
        score += 0.08
    return round(max(0.01, min(0.95, score)), 3)


def compute_preventive_priority(
    *,
    edad: Optional[int],
    grupo_patologia: Optional[str],
    ecog: Optional[str],
    charlson: Optional[str],
    dias_espera: Optional[int],
    requiere_intermed: Optional[str],
) -> Tuple[str, float, str]:
    """Devuelve prioridad clínica, score preventivo y motivo consolidado."""
    score = 0.0
    reasons: List[str] = []

    if (grupo_patologia or "").strip().upper() == "ONCOLOGICO":
        score += 4.0
        reasons.append("PATOLOGIA_ONCOLOGICA")

    ecog_level = extract_numeric_level(ecog)
    if ecog_level >= 2:
        score += 2.5
        reasons.append(f"ECOG_{ecog_level}")
    elif ecog_level == 1:
        score += 1.0
        reasons.append("ECOG_1")

    charlson_level = extract_numeric_level(charlson)
    if charlson_level >= 5:
        score += 3.0
        reasons.append(f"CHARLSON_{charlson_level}")
    elif charlson_level >= 3:
        score += 1.5
        reasons.append(f"CHARLSON_{charlson_level}")
    elif charlson_level > 0:
        score += 0.5
        reasons.append(f"CHARLSON_{charlson_level}")

    if edad is not None:
        if edad >= 80:
            score += 2.0
            reasons.append("EDAD_GE_80")
        elif edad >= 70:
            score += 1.0
            reasons.append("EDAD_GE_70")
        elif edad >= 60:
            score += 0.5
            reasons.append("EDAD_GE_60")

    if dias_espera is not None:
        if dias_espera > 90:
            score += 3.0
            reasons.append("ESPERA_GT_90")
        elif dias_espera > 60:
            score += 2.0
            reasons.append("ESPERA_GT_60")
        elif dias_espera > 30:
            score += 1.0
            reasons.append("ESPERA_GT_30")

    if (requiere_intermed or "").strip().upper() == "SI":
        score += 1.0
        reasons.append("REQUIERE_INTERMED")

    if score >= 8.0:
        priority = "ALTA"
    elif score >= 4.0:
        priority = "MEDIA"
    else:
        priority = "BAJA"

    if not reasons:
        reasons.append("RIESGO_BASAL")
    return priority, round(score, 2), ", ".join(reasons)

