# file: src/venue_enricher/gpt_client.py
from __future__ import annotations
from typing import Dict, Any
from pydantic import BaseModel
from tenacity import retry, stop_after_attempt, wait_exponential
from openai import OpenAI
from .prompts import SYSTEM_PROMPT, build_user_prompt, JSON_SCHEMA

class LocationResult(BaseModel):
    city: str = ""
    country: str = ""
    confidence: float = 0.0
    evidence: str = ""

def normalize_country(country: str) -> str:
    c = (country or "").strip()
    if not c:
        return ""
    lc = c.lower()
    mapping = {
        "usa": "United States",
        "u.s.a.": "United States",
        "u.s.": "United States",
        "us": "United States",
        "united states of america": "United States",
        "uk": "United Kingdom",
        "u.k.": "United Kingdom",
        "england": "United Kingdom",
    }
    if lc in mapping:
        return mapping[lc]
    return " ".join(w.capitalize() for w in lc.split())

class GPTClient:
    # Why JSON schema: minimizes parsing errors, reduces tokens, predictable.
    def __init__(self, api_key: str, model: str, max_tokens: int, prompt_version: str) -> None:
        if not api_key:
            raise ValueError("OPENAI_API_KEY is required")
        self.client = OpenAI(api_key=api_key)
        self.model = model
        self.max_tokens = max_tokens
        self.prompt_version = prompt_version

    @retry(wait=wait_exponential(multiplier=1, min=1, max=30), stop=stop_after_attempt(5))
    def extract(self, row: Dict[str, Any]) -> LocationResult:
        user_prompt = build_user_prompt(row)
        resp = self.client.chat.completions.create(
            model=self.model,
            temperature=0,
            max_tokens=self.max_tokens,
            response_format={"type": "json_schema", "json_schema": JSON_SCHEMA},
            messages=[
                {"role": "system", "content": SYSTEM_PROMPT},
                {"role": "user", "content": user_prompt},
            ],
        )
        content = resp.choices[0].message.content or "{}"
        try:
            import json
            data = json.loads(content)
        except Exception:
            data = {"city": "", "country": "", "confidence": 0.0, "evidence": ""}

        return LocationResult(
            city=(data.get("city") or "").strip(),
            country=normalize_country(data.get("country") or ""),
            confidence=float(data.get("confidence") or 0.0),
            evidence=(data.get("evidence") or "").strip(),
        )
