# path: src/venue_enricher/bq_io.py
from __future__ import annotations

import uuid
from typing import Iterable, Dict, Any, List, Optional
from google.cloud import bigquery


class BigQueryIO:
    """Thin BigQuery IO wrapper used by the service."""

    def __init__(
        self,
        project_id: str,
        dataset_id: str,
        table_id: str,
        location: Optional[str] = None,
    ) -> None:
        self.project_id = project_id
        self.dataset_id = dataset_id
        self.table_id = table_id
        self.client = bigquery.Client(project=project_id)
        self.location = location

    @property
    def fq_table(self) -> str:
        return f"`{self.project_id}.{self.dataset_id}.{self.table_id}`"

    def fetch_rows(self, limit: int, overwrite: bool = False) -> List[Dict[str, Any]]:
        """
        Returns rows to enrich: id, name, address, city, country.
        Why: Ensure we only fetch rows that still need enrichment unless overwrite is set.
        """
        where = (
            "TRUE"
            if overwrite
            else "(enrichment_status = 'OK' AND (city IS NULL OR country IS NULL))"
        )
        sql = f"""
        SELECT id, name, address, city, country
        FROM {self.fq_table}
        WHERE {where}
        ORDER BY RAND()
        LIMIT @limit
        """
        job = self.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("limit", "INT64", limit)],
                use_query_cache=False,
            ),
            location=self.location,
        )
        rows = [dict(r) for r in job.result()]
        return rows

    def update_locations(self, updates: Iterable[Dict[str, Any]], overwrite: bool) -> int:
        """
        Writes {id, city, country} via temp table + MERGE.
        Returns the number of truly modified rows (BigQuery DML affected rows).
        Why: We want the API to report real table mutations so 'pending' visibly drops.
        """
        rows: List[Dict[str, Any]] = []
        for u in updates:
            rid = u.get("id")
            if not rid:
                continue
            city = (u.get("city") or "").strip()
            country = (u.get("country") or "").strip()
            rows.append({"id": str(rid), "city": city, "country": country})

        if not rows:
            return 0

        tmp_name = f"_tmp_enrich_{uuid.uuid4().hex[:8]}"
        tmp_fq = f"`{self.project_id}.{self.dataset_id}.{tmp_name}`"

        # 1) Create temp table
        schema = [
            bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("country", "STRING"),
        ]
        self.client.create_table(
            bigquery.Table(f"{self.project_id}.{self.dataset_id}.{tmp_name}", schema=schema),
            exists_ok=True,
        )

        # 2) Load rows
        load_job = self.client.load_table_from_json(
            rows,
            destination=f"{self.project_id}.{self.dataset_id}.{tmp_name}",
            job_config=bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE"),
            location=self.location,
        )
        load_job.result()

        # 3) MERGE
        # Only update when new non-empty values differ from current values,
        # unless overwrite=True then we allow updates even if target not NULL (still ignore empty strings).
        when_matched_condition = (
            "TRUE"
            if overwrite
            else """
            (
              (NULLIF(S.city, '') IS NOT NULL   AND T.city   IS DISTINCT FROM S.city) OR
              (NULLIF(S.country, '') IS NOT NULL AND T.country IS DISTINCT FROM S.country)
            )
            """
        )

        merge_sql = f"""
        MERGE {self.fq_table} T
        USING {tmp_fq} S
        ON T.id = S.id
        WHEN MATCHED AND {when_matched_condition}
        THEN UPDATE SET
          city    = IFNULL(NULLIF(S.city, ''), T.city),
          country = IFNULL(NULLIF(S.country, ''), T.country)
        """
        qjob = self.client.query(
            merge_sql,
            job_config=bigquery.QueryJobConfig(use_query_cache=False),
            location=self.location,
        )
        qres = qjob.result()  # noqa: F841

        affected = getattr(qjob, "num_dml_affected_rows", None)
        if affected is None:
            # Older client fallback
            stats = getattr(qjob, "dml_statistics", None)
            affected = int(getattr(stats, "modified_row_count", 0)) if stats else 0

        # 4) Cleanup
        self.client.delete_table(
            f"{self.project_id}.{self.dataset_id}.{tmp_name}", not_found_ok=True
        )
        return int(affected or 0)

    def count_pending(self) -> int:
        """No-cache count of pending rows."""
        sql = f"""
        SELECT COUNT(*) AS pending
        FROM {self.fq_table}
        WHERE enrichment_status='OK' AND (city IS NULL OR country IS NULL)
        """
        job = self.client.query(
            sql,
            job_config=bigquery.QueryJobConfig(use_query_cache=False),
            location=self.location,
        )
        return int(list(job.result())[0][0])
