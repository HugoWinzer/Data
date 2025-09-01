# file: src/venue_enricher/prompts.py
from __future__ import annotations
from typing import Dict, Any

SYSTEM_PROMPT = (
    "You extract a venue's physical city and country from noisy metadata. "
    "Return strict JSON per schema. If uncertain, return empty strings and low confidence. "
    "Prefer the venue location over HQ. Avoid guessing."
)

JSON_SCHEMA = {
    "name": "location_schema",
    "schema": {
        "type": "object",
        "properties": {
            "city": {"type": "string"},
            "country": {"type": "string"},
            "confidence": {"type": "number"},
            "evidence": {"type": "string"},
        },
        "required": ["city", "country", "confidence", "evidence"],
        "additionalProperties": False,
    },
}

def build_user_prompt(payload: Dict[str, Any]) -> str:
    parts = [
        f"id: {payload.get('id','')}",
        f"name: {payload.get('name','')}",
        f"alt_name: {payload.get('alt_name','')}",
        f"website_url: {payload.get('website_url','')}",
        f"domain: {payload.get('domain','')}",
        f"linkedin_url: {payload.get('linkedin_url','')}",
        f"phone: {payload.get('phone','')}",
        f"ticket_vendor: {payload.get('ticket_vendor','')}",
        f"ticket_vendor_source: {payload.get('ticket_vendor_source','')}",
        # Brief note can hold any other signals you might add later.
        f"notes: {payload.get('notes','')}",
    ]
    return "\n".join(parts)
