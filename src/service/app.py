# path: src/service/app.py
from __future__ import annotations

import math
import os
from typing import Dict, Any

from fastapi import FastAPI, Query
from venue_enricher.bq_io import BigQueryIO
from venue_enricher.enricher import enrich_batch

app = FastAPI(title="venue-enricher", version="1.0.1")

PROJECT_ID = os.environ.get("PROJECT_ID", "")
DATASET_ID = os.environ.get("DATASET_ID", "")
TABLE_ID = os.environ.get("TABLE_ID", "")
OPENAI_MODEL = os.environ.get("OPENAI_MODEL", "gpt-4o-mini")
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "200"))
CONCURRENCY = int(os.environ.get("CONCURRENCY", "8"))
BQ_LOCATION = os.environ.get("BQ_LOCATION")  # e.g. "US" if your dataset is pinned

bq = BigQueryIO(PROJECT_ID, DATASET_ID, TABLE_ID, location=BQ_LOCATION)


@app.get("/health")
def health() -> Dict[str, Any]:
    return {
        "ok": True,
        "project": PROJECT_ID,
        "dataset": DATASET_ID,
        "table": TABLE_ID,
    }


@app.get("/stats")
def stats() -> Dict[str, Any]:
    # Why: Disable result cache to see fresh counts after MERGE
    pending = bq.count_pending()
    return {"pending": pending}


@app.post("/enrich")
def enrich(
    limit: int = Query(1000, ge=1, le=5000),
    overwrite: bool = Query(False),
    verbose: bool = Query(False),
) -> Dict[str, Any]:
    """
    Returns the actual number of rows modified in BigQuery.
    Why: Affected rows proves pending will drop.
    """
    total_affected = 0
    remaining = limit
    batches = max(1, math.ceil(limit / max(1, BATCH_SIZE)))

    for _ in range(batches):
        n = min(BATCH_SIZE, remaining)
        if n <= 0:
            break

        rows = bq.fetch_rows(limit=n, overwrite=overwrite)
        if not rows:
            break

        updates = enrich_batch(
            rows, model=OPENAI_MODEL, concurrency=CONCURRENCY, verbose=verbose
        )
        affected = bq.update_locations(updates, overwrite=overwrite)
        total_affected += affected
        remaining -= n

        if verbose:
            print({"event": "batch_done", "asked": n, "affected": affected})

    return {
        "updated": total_affected,
        "limit": limit,
        "batch_size": BATCH_SIZE,
        "overwrite": overwrite,
    }
