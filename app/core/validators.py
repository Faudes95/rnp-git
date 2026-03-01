from __future__ import annotations

import re
from typing import Any, Iterable, List, Set

# Tokens frecuentes de captura basura; no bloquean todo el flujo por defecto,
# pero se usan para validación/advertencia centralizada.
_PLACEHOLDER_TOKENS = {
    "N/A",
    "NA",
    "NONE",
    "NULL",
    "SIN DATO",
    "SIN_DATOS",
    "POR DEFINIR",
    "PENDIENTE",
    "TEST",
    "PRUEBA",
    "XXXX",
    "0000000000",
}


def normalize_whitespace(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def normalize_curp(value: Any) -> str:
    return re.sub(r"\s+", "", str(value or "").upper())


def _nss_digits(value: Any) -> str:
    return re.sub(r"\D", "", str(value or ""))


def normalize_nss_10(value: Any, *, strategy: str = "legacy_left") -> str:
    """Normaliza NSS a 10 dígitos preservando compatibilidad histórica.

    strategy:
      - legacy_left: conserva los primeros 10 (comportamiento histórico del proyecto)
      - master_right: conserva los últimos 10 (usado por master identity)
    """
    digits = _nss_digits(value)
    if len(digits) == 10:
        return digits
    if len(digits) < 10:
        return ""
    if strategy == "master_right":
        return digits[-10:]
    return digits[:10]


def nss_aliases(value: Any) -> List[str]:
    """Genera alias para cruces compatibles entre históricos (10/11+ dígitos)."""
    digits = _nss_digits(value)
    aliases: Set[str] = set()
    if len(digits) >= 10:
        aliases.add(digits[:10])
        aliases.add(digits[-10:])
    if len(digits) == 10:
        aliases.add(digits)
    return sorted(a for a in aliases if len(a) == 10)


def is_valid_nss_10(value: Any) -> bool:
    return len(normalize_nss_10(value)) == 10


def is_placeholder_text(value: Any) -> bool:
    txt = normalize_whitespace(value).upper()
    return bool(txt and txt in _PLACEHOLDER_TOKENS)


def filter_empty_values(values: Iterable[Any]) -> List[str]:
    out: List[str] = []
    for v in values:
        txt = normalize_whitespace(v)
        if txt:
            out.append(txt)
    return out
