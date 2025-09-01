# file: src/venue_enricher/config.py
from __future__ import annotations
from dataclasses import dataclass
import os

@dataclass(frozen=True)
class Settings:
    project_id: str = os.getenv("PROJECT_ID", "")
    dataset_id: str = os.getenv("DATASET_ID", "")
    table_id: str = os.getenv("TABLE_ID", "")
    openai_api_key: str = os.getenv("OPENAI_API_KEY", "")
    openai_model: str = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
    max_tokens: int = int(os.getenv("OPENAI_MAX_TOKENS", "160"))
    batch_size: int = int(os.getenv("BATCH_SIZE", "200"))
    concurrency: int = int(os.getenv("CONCURRENCY", "8"))
    prompt_version: str = "v1.0-city-country"
