# file: src/venue_enricher/enricher.py
from __future__ import annotations
from typing import Any, Dict, Iterable, List
from dataclasses import dataclass
import json

from .gpt_client import GPTClient, LocationResult
from .cache import EnrichmentCache


@dataclass
class EnrichStats:
    processed: int = 0
    from_cache: int = 0
    api_calls: int = 0


def _log(event: str, **fields: Any) -> None:
    # One-line JSON; easy to filter in Cloud Logging
    try:
        print(json.dumps({"event": event, **fields}, ensure_ascii=False))
    except Exception:
        print(f"{event} {fields}")


def enrich_rows(
    rows: Iterable[Dict[str, Any]],
    client: GPTClient,
    cache: EnrichmentCache,
    overwrite: bool = False,
    verbose: bool = True,
) -> List[Dict[str, Any]]:
    row_list = list(rows)
    updates: List[Dict[str, Any]] = []
    stats = EnrichStats()
    memo: Dict[str, LocationResult] = {}

    if verbose:
        _log("enrich_start", total=len(row_list), model=client.model)

    for row in row_list:
        stats.processed += 1
        row_id = row.get("id")
        name = (row.get("name") or "")[:200]
        key = cache.make_key(row)

        if not overwrite:
            cached = cache.get(key)
            if cached:
                city, country, confidence, _ = cached
                updates.append({"id": row_id, "city": city, "country": country})
                stats.from_cache += 1
                if verbose:
                    _log("enrich_row", id=row_id, name=name, source="cache",
                         city=city, country=country, confidence=confidence)
                continue

        if key in memo:
            result = memo[key]
            src = "memo"
        else:
            result = client.extract(row)
            cache.put(key, result.city, result.country, result.confidence, result.evidence)
            memo[key] = result
            stats.api_calls += 1
            src = "openai"

        updates.append({"id": row_id, "city": result.city, "country": result.country})
        if verbose:
            _log("enrich_row", id=row_id, name=name, source=src,
                 city=result.city, country=result.country, confidence=result.confidence)

    if verbose:
        _log("enrich_done", processed=stats.processed, from_cache=stats.from_cache,
             api_calls=stats.api_calls, updated=len(updates))

    return updates
