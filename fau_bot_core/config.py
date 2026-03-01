from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class CoreConfig:
    clinical_ro_dsn: str
    surgical_ro_dsn: str
    output_dsn: str
    output_schema: str
    llm_provider: str
    llm_model: str
    llm_base_url: str
    llm_api_key: str


def _env(name: str, default: str = "") -> str:
    return str(os.getenv(name, default)).strip()


def get_config() -> CoreConfig:
    clinical_default = _env("DATABASE_URL", "sqlite:///./urologia.db")
    surgical_default = _env("SURGICAL_DATABASE_URL", "sqlite:///./urologia_quirurgico.db")
    output_default = _env("DATABASE_URL", "sqlite:///./urologia.db")
    return CoreConfig(
        clinical_ro_dsn=_env("FAU_CORE_CLINICAL_RO_DSN", clinical_default),
        surgical_ro_dsn=_env("FAU_CORE_SURGICAL_RO_DSN", surgical_default),
        output_dsn=_env("FAU_CORE_OUTPUT_DSN", output_default),
        output_schema=_env("FAU_CORE_OUTPUT_SCHEMA", "fau_bot_out"),
        llm_provider=_env("FAU_CORE_LLM_PROVIDER", "none").lower(),
        llm_model=_env("FAU_CORE_LLM_MODEL", "llama3.1:8b-instruct-q4_K_M"),
        llm_base_url=_env("FAU_CORE_LLM_BASE_URL", "http://127.0.0.1:11434"),
        llm_api_key=_env("FAU_CORE_LLM_API_KEY", ""),
    )
