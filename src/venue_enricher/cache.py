# file: src/venue_enricher/cache.py
from __future__ import annotations
import hashlib
import json
import os
import sqlite3
from typing import Any, Dict, Optional, Tuple

class EnrichmentCache:
    # Writes to /tmp (writable in Cloud Run)
    def __init__(self, path: str = "/tmp/enrichment_cache.sqlite") -> None:
        self.path = path
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.conn = sqlite3.connect(path)
        self._init()

    def _init(self) -> None:
        cur = self.conn.cursor()
        cur.execute("""
            CREATE TABLE IF NOT EXISTS cache (
                key TEXT PRIMARY KEY,
                city TEXT,
                country TEXT,
                confidence REAL,
                evidence TEXT
            )
        """)
        self.conn.commit()

    @staticmethod
    def make_key(row: Dict[str, Any]) -> str:
        payload = {
            k: row.get(k, "")
            for k in ["name","alt_name","website_url","domain","linkedin_url","phone"]
        }
        blob = json.dumps(payload, sort_keys=True, ensure_ascii=False)
        return hashlib.sha256(blob.encode("utf-8")).hexdigest()

    def get(self, key: str) -> Optional[Tuple[str, str, float, str]]:
        cur = self.conn.cursor()
        cur.execute("SELECT city,country,confidence,evidence FROM cache WHERE key=?", (key,))
        r = cur.fetchone()
        return tuple(r) if r else None

    def put(self, key: str, city: str, country: str, confidence: float, evidence: str) -> None:
        cur = self.conn.cursor()
        cur.execute(
            "REPLACE INTO cache(key,city,country,confidence,evidence) VALUES (?,?,?,?,?)",
            (key, city, country, confidence, evidence),
        )
        self.conn.commit()
