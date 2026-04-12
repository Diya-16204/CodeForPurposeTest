# /* * ISA Standard Compliant
#  * Distributed under the Apache License, Version 2.0.
#  * SPDX-License-Identifier: Apache-2.0
#  */
from dataclasses import dataclass
from functools import lru_cache
import os
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    storage_dir: Path
    max_analysis_rows: int
    llm_provider: str
    gemini_api_key: str
    gemini_model: str
    groq_api_key: str
    groq_model: str


def _project_root() -> Path:
    return Path(__file__).resolve().parents[1]


def _storage_dir() -> Path:
    raw_path = Path(os.getenv("AI_STORAGE_DIR", "storage/datasets"))
    if raw_path.is_absolute():
        return raw_path
    return (_project_root() / raw_path).resolve()


def _positive_int(name: str, fallback: int) -> int:
    raw_value = os.getenv(name)
    if not raw_value:
        return fallback

    parsed = int(raw_value)
    if parsed <= 0:
        raise ValueError(f"{name} must be a positive integer.")
    return parsed


@lru_cache
def get_settings() -> Settings:
    storage_dir = _storage_dir()
    storage_dir.mkdir(parents=True, exist_ok=True)

    return Settings(
        gemini_api_key=os.getenv("GEMINI_API_KEY", ""),
        gemini_model=os.getenv("GEMINI_MODEL", ""),
        groq_api_key=os.getenv("GROQ_API_KEY", ""),
        groq_model=os.getenv("GROQ_MODEL", ""),
        llm_provider=os.getenv("LLM_PROVIDER", "none").lower(),
        max_analysis_rows=_positive_int("MAX_ANALYSIS_ROWS", 10000),
        storage_dir=storage_dir,
    )
