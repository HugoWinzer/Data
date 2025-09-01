# path: src/venue_enricher/enricher.py
from __future__ import annotations

import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Dict, Any, Iterable, List, Tuple, Optional

try:
    from openai import OpenAI  # lazy import handling below
except Exception:  # pragma: no cover
    OpenAI = None  # type: ignore


CITY_HINTS = re.compile(
    r"\b(?:city|town|municipality|locality|metropolis|ville)\b", re.I
)
COUNTRY_WORDS = {
    "usa": "United States",
    "u.s.a.": "United States",
    "us": "United States",
    "u.s.": "United States",
    "uk": "United Kingdom",
    "u.k.": "United Kingdom",
    "uae": "United Arab Emirates",
    "u.a.e.": "United Arab Emirates",
}


def _fallback_extract(name: str, address: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Heuristic extractor used when no OpenAI key is present or model fails.
    Why: Keeps pipeline productive even under rate/availability issues.
    """
    text = f"{name or ''} | {address or ''}".strip()
    if not text:
        return None, None

    # Country detection from trailing token / known acronyms
    parts = [p.strip(", ") for p in text.split(",") if p.strip()]
    country = None
    if parts:
        tail = parts[-1].lower()
        country = COUNTRY_WORDS.get(tail)
        if not country and len(tail) > 3:
            country = tail.title() if " " in tail else None

    # City: prefer penultimate token if present
    city = None
    if len(parts) >= 2:
        cand = parts[-2].strip()
        if cand and not CITY_HINTS.search(cand):
            city = cand

    return city, country


def _openai_client() -> Optional["OpenAI"]:
    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key or OpenAI is None:
        return None
    return OpenAI(api_key=api_key)


def _ask_model(client: "OpenAI", model: str, name: str, address: str) -> Tuple[Optional[str], Optional[str]]:
    prompt = (
        "Extract city and country from the venue data. "
        "Return strictly JSON with keys city and country. "
        f"Name: {name!r}\nAddress: {address!r}"
    )
    resp = client.chat.completions.create(
        model=model,
        messages=[
            {"role": "system", "content": "You return only strict JSON."},
            {"role": "user", "content": prompt},
        ],
        temperature=0.0,
    )
    content = resp.choices[0].message.content.strip()
    # Minimal parse without extra deps
    city, country = None, None
    m_city = re.search(r'"city"\s*:\s*"([^"]*)"', content)
    m_country = re.search(r'"country"\s*:\s*"([^"]*)"', content)
    if m_city:
        city = m_city.group(1).strip() or None
    if m_country:
        country = m_country.group(1).strip() or None
    return city, country


def enrich_batch(
    rows: Iterable[Dict[str, Any]],
    model: str,
    concurrency: int = 8,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """
    Returns [{id, city, country}, ...].
    Why: Keep API payload small and let BigQuery do the merge.
    """
    client = _openai_client()
    results: List[Dict[str, Any]] = []

    def _enrich_row(row: Dict[str, Any]) -> Dict[str, Any]:
        rid = row.get("id")
        name = row.get("name") or ""
        address = row.get("address") or ""
        city, country = None, None

        if client:
            try:
                city, country = _ask_model(client, model, name, address)
            except Exception:
                city, country = _fallback_extract(name, address)
        else:
            city, country = _fallback_extract(name, address)

        # Trim empties to "" to let MERGE guards ignore them
        out = {
            "id": rid,
            "city": (city or "").strip(),
            "country": (country or "").strip(),
        }
        if verbose:
            print({"event": "enrich_row", "id": rid, "city": out["city"], "country": out["country"]})
        return out

    with ThreadPoolExecutor(max_workers=max(1, concurrency)) as ex:
        futs = [ex.submit(_enrich_row, r) for r in rows]
        for f in as_completed(futs):
            results.append(f.result())

    return results
