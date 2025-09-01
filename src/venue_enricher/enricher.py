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
    """Emit one-line JSON for Cloud Logging. Why: easy to filter/parse."""
    try:
        print(json.dumps({"event": event, **fields}, ensure_ascii=False))
    except Exception:
        # Fallback plain text if any value is not JSON-serializable.
        print(f"{event} {fields}")


def enrich_rows(
    rows: Iterable[Dict[str, Any]],
    client: GPTClient,
    cache: EnrichmentCache,
    overwrite: bool = False,
    verbose: bool = False,
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
        name = row.get("name") or ""
        key = cache.make_key(row)

        source = "openai"
        if not overwrite:
            cached = cache.get(key)
            if cached:
                city, country, confidence, evidence = cached
                stats.from_cache += 1
                source = "cache"
                updates.append({"id": row_id, "city": city, "country": country})
                if verbose:
                    _log(
                        "enrich_row",
                        id=row_id,
                        name=name,
                        source=source,
                        city=city,
                        country=country,
                        confidence=confidence,
                    )
                continue

        if key in memo:
            result = memo[key]
            source = "memo"
        else:
            result = client.extract(row)
            cache.put(key, result.city, result.country, result.confidence, result.evidence)
            memo[key] = result
            stats.api_calls += 1
            source = "openai"

        updates.append({"id": row_id, "city": result.city, "country": result.country})
        if verbose:
            _log(
                "enrich_row",
                id=row_id,
                name=name,
                source=source,
                city=result.city,
                country=result.country,
                confidence=result.confidence,
            )

    if verbose:
        _log(
            "enrich_done",
            processed=stats.processed,
            from_cache=stats.from_cache,
            api_calls=stats.api_calls,
            updated=len(updates),
        )

    return updates
