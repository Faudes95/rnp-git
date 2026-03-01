from __future__ import annotations

from typing import Any


def request_is_https(request: Any, *, trust_forwarded_proto: bool = True) -> bool:
    if str(request.url.scheme).lower() == "https":
        return True
    if trust_forwarded_proto:
        forwarded_proto = (request.headers.get("x-forwarded-proto", "") or "").lower()
        if "https" in forwarded_proto:
            return True
    return False
