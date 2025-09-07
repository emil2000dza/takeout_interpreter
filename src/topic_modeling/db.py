import json
from typing import Dict, Iterator, Optional, Sequence, Set

from sqlalchemy import text

from src.db.snowflake_client import SnowflakeORM
from src.db.tables import ChromeHistory
from src.topic_modeling.data_models import HistoryEntry


class SnowflakeRepository:
    """Thin repository around SnowflakeORM.

    - Owns a single session (context-managed)
    - Exposes helpers for common patterns
    - Allows both ORM queries and Core text() SQL
    """

    def __init__(self, orm: Optional[SnowflakeORM] = None):
        self._orm = orm or SnowflakeORM()
        self.session = None

    def __enter__(self) -> "SnowflakeRepository":
        self._ctx = self._orm.session_scope()
        self.session = self._ctx.__enter__()
        return self

    def __exit__(self, exc_type, exc, tb):
        self._ctx.__exit__(exc_type, exc, tb)
        self.session = None

    # ---- Helpers ----
    def ensure_classification_table(self, table: str) -> None:
        self.session.execute(
            text(
                f"""
                CREATE TABLE IF NOT EXISTS {table} (
                    title STRING,
                    url STRING,
                    topics ARRAY
                )
                """
            )
        )

    def distinct_classified_urls(self, table: str) -> Set[str]:
        rows = self.session.execute(text(f"SELECT url FROM {table}")).fetchall()
        return {r[0] for r in rows}

    def count_classified_urls(self, table: str) -> int:
        row = self.session.execute(text(f"SELECT COUNT(DISTINCT url) FROM {table}")).fetchone()
        return (row[0] or 0) if row else 0

    def write_classifications(self, table: str, entries: Sequence[Dict[str, object]]) -> None:
        if not entries:
            return
        sql = text(
            f"""
            INSERT INTO {table} (title, url, topics)
            SELECT $1, $2, PARSE_JSON($3)
            FROM VALUES (:title, :url, :topics)
            """
        )
        params = [
            {"title": e["title"], "url": e["url"], "topics": json.dumps(e.get("topics", []))}
            for e in entries
        ]
        self.session.execute(sql, params)

    def fetch_history_by_domain(self, domain: str, limit: Optional[int] = None) -> Iterator[HistoryEntry]:
        q = (
            self.session.query(ChromeHistory.title, ChromeHistory.url)
            .filter(ChromeHistory.domain == domain)
        )
        if limit:
            q = q.limit(limit)
        # Stream to avoid high memory; iterate and yield
        for row in q:
            yield HistoryEntry(title=row.title, url=row.url)
