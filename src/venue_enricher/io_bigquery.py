# file: src/venue_enricher/io_bigquery.py
from __future__ import annotations
from typing import Any, Dict, List
from google.cloud import bigquery as bq

SELECT_TEMPLATE = """
SELECT id, name, alt_name, website_url, domain, linkedin_url, phone, ticket_vendor, ticket_vendor_source
FROM `{project}.{dataset}.{table}`
WHERE enrichment_status='OK' AND (city IS NULL OR country IS NULL)
ORDER BY id
LIMIT {limit}
"""

def fetch_rows(project: str, dataset: str, table: str, limit: int) -> List[Dict[str, Any]]:
    client = bq.Client(project=project)
    sql = SELECT_TEMPLATE.format(project=project, dataset=dataset, table=table, limit=limit)
    res = client.query(sql).result()
    return [dict(r) for r in res]

def update_locations(project: str, dataset: str, table: str, updates: List[Dict[str, Any]]) -> int:
    if not updates:
        return 0
    client = bq.Client(project=project)

    temp = f"{project}.{dataset}._tmp_city_country"
    schema = [
        bq.SchemaField("id", "STRING"),
        bq.SchemaField("city", "STRING"),
        bq.SchemaField("country", "STRING"),
    ]
    job = client.load_table_from_json(updates, temp, job_config=bq.LoadJobConfig(schema=schema, write_disposition="WRITE_TRUNCATE"))
    job.result()

    merge_sql = f"""
    MERGE `{project}.{dataset}.{table}` T
    USING `{temp}` S
    ON T.id = S.id
    WHEN MATCHED THEN UPDATE SET
      T.city = S.city,
      T.country = S.country
    """
    client.query(merge_sql).result()
    client.delete_table(temp, not_found_ok=True)
    return len(updates)
