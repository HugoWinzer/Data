# file: src/venue_enricher/enricher.py
from __future__ import annotations
from typing import Any, Dict, Iterable, List
from dataclasses import dataclass
from tqdm import tqdm
from .gpt_client import GPTClient, LocationResult
from .cache import EnrichmentCache

@dataclass
class EnrichStats:
    processed: int = 0
    from_cache: int = 0
    api_calls: int = 0

def enrich_rows(rows: Iterable[Dict[str, Any]], client: GPTClient, cache: EnrichmentCache, overwrite: bool = False) -> List[Dict[str, Any]]:
    updates: List[Dict[str, Any]] = []
    stats = EnrichStats()

    # Small in-memory memo to catch repeats within the same batch
    memo: Dict[str, LocationResult] = {}

    for row in tqdm(list(rows), desc="enrich", unit="row"):
        stats.processed += 1
        key = cache.make_key(row)

        if not overwrite:
            cached = cache.get(key)
            if cached:
                city, country, confidence, evidence = cached
                stats.from_cache += 1
                updates.append({"id": row.get("id"), "city": city, "country": country})
                memo[key] = LocationResult(city=city, country=country, confidence=confidence, evidence=evidence)
                continue

        if key in memo:
            result = memo[key]
        else:
            result = client.extract(row)
            cache.put(key, result.city, result.country, result.confidence, result.evidence)
            memo[key] = result
            stats.api_calls += 1

        updates.append({"id": row.get("id"), "city": result.city, "country": result.country})

    return updates
