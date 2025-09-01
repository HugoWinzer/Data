# file: src/venue_enricher/io_bigquery.py
from __future__ import annotations
from typing import Any, Dict, List
from google.cloud import bigquery as bq


def _table(project_id: str, dataset_id: str, table_id: str) -> str:
    return f"{project_id}.{dataset_id}.{table_id}"


def fetch_rows(project_id: str, dataset_id: str, table_id: str, limit: int) -> List[Dict[str, Any]]:
    """Return rows needing enrichment. Why: pull only fields useful for GPT + key."""
    client = bq.Client(project=project_id)
    sql = f"""
    SELECT
      id, name, alt_name, website_url, domain, linkedin_url, phone
    FROM `{_table(project_id, dataset_id, table_id)}`
    WHERE enrichment_status = 'OK' AND (city IS NULL OR country IS NULL)
    LIMIT @limit
    """
    job = client.query(
        sql,
        job_config=bq.QueryJobConfig(
            query_parameters=[bq.ScalarQueryParameter("limit", "INT64", limit)]
        ),
    )
    rows: List[Dict[str, Any]] = []
    for row in job.result():
        rows.append({k: row[k] for k in row.keys()})
    return rows


def update_locations(
    project_id: str,
    dataset_id: str,
    table_id: str,
    updates: List[Dict[str, Any]],
) -> int:
    """
    Update city/country using a single DML statement with an array-of-struct param.
    Why: ensures `id` (STRING) matches correctly and returns *actual* affected rows.
    """
    if not updates:
        return 0

    # Build (id, city, country) tuples; force id to STRING
    values = [(str(u["id"]), u.get("city"), u.get("country")) for u in updates]

    client = bq.Client(project=project_id)
    job_config = bq.QueryJobConfig(
        query_parameters=[
            bq.ArrayQueryParameter(
                "rows",
                "STRUCT<id STRING, city STRING, country STRING>",
                values,
            )
        ]
    )

    sql = f"""
    UPDATE `{_table(project_id, dataset_id, table_id)}` T
    SET
      T.city = R.city,
      T.country = R.country,
      T.last_updated = CURRENT_TIMESTAMP()
    FROM UNNEST(@rows) AS R
    WHERE T.id = R.id
    """

    job = client.query(sql, job_config=job_config)
    job.result()  # wait for completion
    return int(job.num_dml_affected_rows or 0)
