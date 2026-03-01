import json
import logging
import re
from typing import Any, Dict

from .config import PII_FIELDS
from .time_utils import utcnow

CURP_RE = re.compile(r"\b[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]\d\b", re.IGNORECASE)
NSS_RE = re.compile(r"\b\d{10,11}\b")
EMAIL_RE = re.compile(r"\b[^\s@]+@[^\s@]+\.[^\s@]+\b")
PHONE_RE = re.compile(r"\b\d{10}\b")


def redact_value(value: Any) -> Any:
    if value is None:
        return None
    if isinstance(value, dict):
        return redact_dict(value)
    if isinstance(value, list):
        return [redact_value(v) for v in value]
    if not isinstance(value, str):
        return value

    text = CURP_RE.sub("***CURP***", value)
    text = NSS_RE.sub("***NSS***", text)
    text = EMAIL_RE.sub("***EMAIL***", text)
    text = PHONE_RE.sub("***TEL***", text)
    return text


def redact_dict(data: Dict[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for key, val in data.items():
        if key.lower() in PII_FIELDS:
            out[key] = "***REDACTED***"
        else:
            out[key] = redact_value(val)
    return out


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: Dict[str, Any] = {
            "timestamp": utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": redact_value(record.getMessage()),
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


class RedactionFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        try:
            if isinstance(record.msg, dict):
                record.msg = redact_dict(record.msg)
            else:
                record.msg = redact_value(str(record.msg))
            if record.args:
                record.args = tuple(redact_value(a) for a in record.args)
        except Exception:
            pass
        return True


_configured = False


def configure_structured_logging(level: int = logging.INFO) -> None:
    global _configured
    if _configured:
        return

    handler = logging.StreamHandler()
    handler.setFormatter(JsonFormatter())
    handler.addFilter(RedactionFilter())

    root = logging.getLogger()
    root.setLevel(level)

    # Reemplazo controlado de handlers para evitar doble salida.
    root.handlers = [handler]

    _configured = True
