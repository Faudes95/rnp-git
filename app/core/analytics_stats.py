from __future__ import annotations

import math
import re
from datetime import date, datetime
from typing import Any, Dict, List, Optional


def parse_any_date(raw_value: Any) -> Optional[date]:
    if raw_value is None:
        return None
    if isinstance(raw_value, date):
        return raw_value
    txt = str(raw_value).strip()
    if not txt:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(txt, fmt).date()
        except Exception:
            continue
    return None


def calc_percentile(values: List[float], percentile: float) -> Optional[float]:
    if not values:
        return None
    ordered = sorted(float(v) for v in values)
    if len(ordered) == 1:
        return round(ordered[0], 2)
    pct = max(0.0, min(100.0, float(percentile)))
    pos = (len(ordered) - 1) * (pct / 100.0)
    left = int(math.floor(pos))
    right = int(math.ceil(pos))
    if left == right:
        return round(ordered[left], 2)
    value = ordered[left] + (ordered[right] - ordered[left]) * (pos - left)
    return round(value, 2)


def safe_pct(num: int, den: int) -> float:
    if den <= 0:
        return 0.0
    return round((float(num) / float(den)) * 100.0, 2)


def parse_lab_numeric(raw_value: Any) -> Optional[float]:
    if raw_value is None:
        return None
    text_value = str(raw_value).strip().replace(",", "")
    if not text_value:
        return None
    match = re.search(r"-?\d+(?:\.\d+)?", text_value)
    if not match:
        return None
    try:
        return float(match.group(0))
    except Exception:
        return None


def lab_key_from_text(test_name: Optional[str], test_code: Optional[str]) -> Optional[str]:
    txt = f"{(test_name or '').lower()} {(test_code or '').lower()}"
    if re.search(r"clostr|c\W*difficile|c\W*dif", txt):
        return "clostridium"
    if re.search(r"creatin|(^|\\W)cr(\\W|$)", txt):
        return "creatinina"
    if re.search(r"hemoglob|(^|\\W)hb(\\W|$)|(^|\\W)hgb(\\W|$)", txt):
        return "hemoglobina"
    if re.search(r"leuc|wbc|leuko", txt):
        return "leucocitos"
    if re.search(r"plaquet|(^|\\W)plt(\\W|$)", txt):
        return "plaquetas"
    if re.search(r"sodio|(^|\\W)na(\\W|$)", txt):
        return "sodio"
    if re.search(r"potasio|(^|\\W)k(\\W|$)", txt):
        return "potasio"
    return None


def lab_positive_clostridium(raw_value: Any) -> bool:
    txt = (str(raw_value or "")).strip().lower()
    if not txt:
        return False
    if re.search(r"negativ|no detect|ausente|no react", txt):
        return False
    if re.search(r"positiv|detect|reactiv|toxina", txt):
        return True
    numeric = parse_lab_numeric(txt)
    if numeric is not None and numeric > 0:
        return True
    return False


def hospital_stay_days(row: Any, today_value: date) -> Optional[int]:
    if row is None:
        return None
    days_h = getattr(row, "dias_hospitalizacion", None)
    if days_h is not None:
        return int(days_h)
    ingreso = getattr(row, "fecha_ingreso", None)
    if ingreso is None:
        return None
    end_date = getattr(row, "fecha_egreso", None) or today_value
    try:
        return max((end_date - ingreso).days, 0)
    except Exception:
        return None


def distribution_stats_table(group_to_values: Dict[str, List[float]], max_items: int = 20) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for group_key, values in group_to_values.items():
        nums = [float(v) for v in values if v is not None]
        if not nums:
            continue
        rows.append(
            {
                "grupo": group_key or "NO_REGISTRADO",
                "n": len(nums),
                "promedio": round(sum(nums) / len(nums), 2),
                "mediana": calc_percentile(nums, 50),
                "p90": calc_percentile(nums, 90),
            }
        )
    rows.sort(key=lambda r: (-int(r.get("n") or 0), str(r.get("grupo") or "")))
    return rows[: max(1, int(max_items))]


def as_date(raw_value: Any) -> Optional[date]:
    if raw_value is None:
        return None
    if isinstance(raw_value, date):
        return raw_value
    if isinstance(raw_value, datetime):
        return raw_value.date()
    txt = str(raw_value).strip()
    if not txt:
        return None
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(txt, fmt).date()
        except Exception:
            continue
    return None
