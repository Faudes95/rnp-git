"""Servicios analíticos extraídos progresivamente."""

import base64
import io
from datetime import date
from typing import Any, Callable, Dict, Iterable, List, Optional, Tuple


def count_values(values: Iterable[str]) -> Dict[str, int]:
    out: Dict[str, int] = {}
    for value in values:
        key = value or "NO_REGISTRADO"
        out[key] = out.get(key, 0) + 1
    return out


def sort_counts_desc(counts: Dict[str, int]) -> Iterable[Tuple[str, int]]:
    return sorted(counts.items(), key=lambda x: (-x[1], x[0]))


def kaplan_meier(durations: List[int], events: List[int]) -> Tuple[List[int], List[float]]:
    data = sorted(zip(durations, events), key=lambda x: x[0])
    at_risk = len(data)
    survival = 1.0
    times: List[int] = []
    surv_values: List[float] = []
    last_time = None
    for t, e in data:
        if last_time is None or t != last_time:
            times.append(t)
            surv_values.append(survival)
            last_time = t
        if e == 1:
            survival *= (at_risk - 1) / at_risk
        at_risk -= 1
        if at_risk <= 0:
            break
    return times, surv_values


def resolve_survival_event(
    consulta: Any,
    *,
    event_field: str,
    event_value: Optional[str],
) -> Tuple[bool, Optional[date]]:
    value = getattr(consulta, event_field, None) if hasattr(consulta, event_field) else None
    if value is None:
        return False, None
    if event_value:
        event = str(value).lower() == str(event_value).lower()
    else:
        event = bool(value)
    event_date = getattr(consulta, "fecha_evento", None) or getattr(consulta, "fecha_registro", None)
    return event, event_date


def fig_to_base64(fig: Any) -> str:
    buf = io.BytesIO()
    fig.savefig(buf, format="png", bbox_inches="tight")
    buf.seek(0)
    return base64.b64encode(buf.read()).decode("utf-8")


def count_by(rows: List[Any], key_getter: Callable[[Any], Any]) -> List[Tuple[str, int]]:
    counts: Dict[str, int] = {}
    for row in rows:
        key = key_getter(row)
        key_norm = str(key).strip() if key is not None else "NO_REGISTRADO"
        key_norm = key_norm if key_norm else "NO_REGISTRADO"
        counts[key_norm] = counts.get(key_norm, 0) + 1
    return sorted(counts.items(), key=lambda x: (-x[1], x[0]))


def build_programmed_age_counts(
    rows: List[Any],
    *,
    age_buckets: List[str],
    classify_age_group_fn: Callable[[Any], str],
) -> List[Tuple[str, int]]:
    counts: Dict[str, int] = {bucket: 0 for bucket in age_buckets}
    extras: Dict[str, int] = {}
    for row in rows:
        bucket = classify_age_group_fn(getattr(row, "edad", None))
        if bucket in counts:
            counts[bucket] += 1
        else:
            extras[bucket] = extras.get(bucket, 0) + 1
    result = [(bucket, counts[bucket]) for bucket in age_buckets]
    for key in sorted(extras.keys()):
        result.append((key, extras[key]))
    return result
