# file: src/service/app.py
from __future__ import annotations
from fastapi import FastAPI, HTTPException, Query
from typing import Optional
from venue_enricher.config import Settings
from venue_enricher.gpt_client import GPTClient
from venue_enricher.cache import EnrichmentCache
from venue_enricher.enricher import enrich_rows
from venue_enricher import io_bigquery as bqio

app = FastAPI(title="Venue City/Country Enricher", version="1.0.0")
cfg = Settings()

@app.get("/health")
def health():
    return {"ok": True, "project": cfg.project_id, "dataset": cfg.dataset_id, "table": cfg.table_id}

@app.post("/enrich")
def run_enrichment(limit: int = Query(30000, ge=1, le=100000), overwrite: bool = Query(False)):
    if not (cfg.project_id and cfg.dataset_id and cfg.table_id):
        raise HTTPException(status_code=500, detail="PROJECT_ID/DATASET_ID/TABLE_ID must be set")

    rows = bqio.fetch_rows(cfg.project_id, cfg.dataset_id, cfg.table_id, limit)
    if not rows:
        return {"updated": 0, "message": "No rows to enrich (all set or filter mismatched)."}

    client = GPTClient(
        api_key=cfg.openai_api_key,
        model=cfg.openai_model,
        max_tokens=cfg.max_tokens,
        prompt_version=cfg.prompt_version,
    )
    cache = EnrichmentCache()

    updates = enrich_rows(rows, client, cache, overwrite=overwrite)

    # Persist in batches
    updated_total = 0
    start = 0
    batch_size = cfg.batch_size
    while start < len(updates):
        chunk = updates[start : start + batch_size]
        updated_total += bqio.update_locations(cfg.project_id, cfg.dataset_id, cfg.table_id, chunk)
        start += batch_size

    return {"updated": updated_total, "limit": limit, "batch_size": batch_size}
