# file: src/service/app.py
from __future__ import annotations
from fastapi import FastAPI, HTTPException, Query
from google.cloud import bigquery as bq

from venue_enricher.config import Settings
from venue_enricher.gpt_client import GPTClient
from venue_enricher.cache import EnrichmentCache
from venue_enricher.enricher import enrich_rows
from venue_enricher import io_bigquery as bqio

app = FastAPI(title="Venue City/Country Enricher", version="1.2.0")
cfg = Settings()


@app.get("/health")
def health():
    return {"ok": True, "project": cfg.project_id, "dataset": cfg.dataset_id, "table": cfg.table_id}


@app.get("/stats")
def stats():
    if not (cfg.project_id and cfg.dataset_id and cfg.table_id):
        raise HTTPException(status_code=500, detail="PROJECT_ID/DATASET_ID/TABLE_ID must be set")
    client = bq.Client(project=cfg.project_id)
    sql = f"""
      SELECT COUNT(*) AS c
      FROM `{cfg.project_id}.{cfg.dataset_id}.{cfg.table_id}`
      WHERE enrichment_status='OK' AND (city IS NULL OR country IS NULL)
    """
    c = client.query(sql).result().to_dataframe()["c"][0]
    return {"pending": int(c)}


@app.post("/enrich")
def run_enrichment(
    limit: int = Query(30000, ge=1, le=100000),
    overwrite: bool = Query(False),
    verbose: bool = Query(True),
):
    if not (cfg.project_id and cfg.dataset_id and cfg.table_id):
        raise HTTPException(status_code=500, detail="PROJECT_ID/DATASET_ID/TABLE_ID must be set")

    rows = bqio.fetch_rows(cfg.project_id, cfg.dataset_id, cfg.table_id, limit)
    if not rows:
        return {"updated": 0, "message": "No rows to enrich."}

    client = GPTClient(
        api_key=cfg.openai_api_key,
        model=cfg.openai_model,
        max_tokens=cfg.max_tokens,
        prompt_version=cfg.prompt_version,
    )
    cache = EnrichmentCache()

    updates = enrich_rows(rows, client, cache, overwrite=overwrite, verbose=verbose)

    updated_total = 0
    start = 0
    batch_size = cfg.batch_size  # 200 by env
    while start < len(updates):
        chunk = updates[start : start + batch_size]
        affected = bqio.update_locations(cfg.project_id, cfg.dataset_id, cfg.table_id, chunk)
        updated_total += affected
        start += batch_size

    return {"updated": updated_total, "limit": limit, "batch_size": batch_size}
