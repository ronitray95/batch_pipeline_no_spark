import sqlite3
import os


class DedupService:
    """
    Disk-backed deduplication service using SQLite.
    """

    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path), exist_ok=True)

        self.conn = sqlite3.connect(path)
        self._init_table()

    def _init_table(self):
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS seen (
                order_id TEXT PRIMARY KEY
            )
            """
        )
        self.conn.commit()

    def is_duplicate(self, order_id: str) -> bool:
        cur = self.conn.execute(
            "SELECT 1 FROM seen WHERE order_id = ?",
            (order_id,)
        )
        return cur.fetchone() is not None

    def mark_seen(self, order_id: str):
        self.conn.execute(
            "INSERT OR IGNORE INTO seen VALUES (?)",
            (order_id,)
        )
        self.conn.commit()

    def close(self):
        self.conn.close()
