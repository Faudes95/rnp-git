from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Dict, Optional

from .config import get_config

try:
    import httpx
except Exception:
    httpx = None


CURP_RE = re.compile(r"\b[A-Z]{4}\d{6}[HM][A-Z]{5}[A-Z0-9]{2}\b", re.IGNORECASE)
NSS_RE = re.compile(r"\b\d{10}\b")
EMAIL_RE = re.compile(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}")
PHONE_RE = re.compile(r"\b\d{10}\b")


@dataclass
class LLMOutput:
    text: str
    provider: str
    model: str
    used_remote: bool
    guardrails: Dict[str, str]


def _mask_phi(text: str) -> str:
    out = CURP_RE.sub("[CURP]", text or "")
    out = NSS_RE.sub("[NSS]", out)
    out = EMAIL_RE.sub("[EMAIL]", out)
    out = PHONE_RE.sub("[PHONE]", out)
    return out


def _guardrail_check(text: str) -> Dict[str, str]:
    t = (text or "").lower()
    flags: Dict[str, str] = {}
    if "diagnostico definitivo" in t:
        flags["diagnostico"] = "Evitar dictamen definitivo sin validación médica presencial."
    if "suspender" in t and "inmediatamente" in t:
        flags["suspension"] = "No suspender tratamientos críticos sin revisión humana."
    if "dosis" in t and any(x in t for x in ["mg", "ml"]):
        flags["dosificacion"] = "Revisar dosis con médico tratante; IA solo sugiere, no prescribe."
    return flags


class LocalLLMClient:
    def __init__(self) -> None:
        cfg = get_config()
        self.provider = cfg.llm_provider
        self.model = cfg.llm_model
        self.base_url = cfg.llm_base_url.rstrip("/")
        self.api_key = cfg.llm_api_key

    def _call_ollama(self, prompt: str) -> Optional[str]:
        if httpx is None:
            return None
        url = f"{self.base_url}/api/generate"
        payload = {
            "model": self.model,
            "prompt": prompt,
            "stream": False,
            "options": {"temperature": 0.1},
        }
        try:
            with httpx.Client(timeout=30.0) as client:
                resp = client.post(url, json=payload)
                resp.raise_for_status()
                data = resp.json()
                return str(data.get("response") or "").strip() or None
        except Exception:
            return None

    def _call_vllm(self, prompt: str) -> Optional[str]:
        if httpx is None:
            return None
        url = f"{self.base_url}/v1/chat/completions"
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        payload = {
            "model": self.model,
            "messages": [
                {
                    "role": "system",
                    "content": "Eres asistente clínico institucional. No des diagnóstico definitivo ni prescripción cerrada.",
                },
                {"role": "user", "content": prompt},
            ],
            "temperature": 0.1,
            "max_tokens": 500,
        }
        try:
            with httpx.Client(timeout=40.0) as client:
                resp = client.post(url, headers=headers, json=payload)
                resp.raise_for_status()
                data = resp.json()
                choices = data.get("choices") or []
                if not choices:
                    return None
                msg = choices[0].get("message") or {}
                return str(msg.get("content") or "").strip() or None
        except Exception:
            return None

    def summarize(self, text: str, task: str = "clinical_summary") -> LLMOutput:
        masked = _mask_phi(text)
        prompt = (
            "Genera un resumen técnico para comité clínico en español. "
            "Incluye: hallazgos clave, riesgos, y recomendaciones para revisión humana. "
            f"Tarea={task}.\n\nTexto:\n{masked}"
        )

        used_remote = False
        content: Optional[str] = None
        if self.provider == "ollama":
            content = self._call_ollama(prompt)
            used_remote = content is not None
        elif self.provider == "vllm":
            content = self._call_vllm(prompt)
            used_remote = content is not None

        if not content:
            content = (
                "Resumen automático (fallback): revisar hallazgos, priorizar eventos de alto riesgo, "
                "y confirmar toda recomendación con validación médica humana."
            )

        flags = _guardrail_check(content)
        if flags:
            content += "\n\n[Guardrails] " + json.dumps(flags, ensure_ascii=False)

        return LLMOutput(
            text=content,
            provider=self.provider,
            model=self.model,
            used_remote=used_remote,
            guardrails=flags,
        )
