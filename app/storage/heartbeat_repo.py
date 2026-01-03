from dataclasses import dataclass
from typing import Any, Optional
import time

@dataclass
class HeartbeatRepository:
    conn: Any
    source: str = "okx"

    def beat(self, service: str, ts_ms: Optional[int] = None) -> None:
        if ts_ms is None:
            ts_ms = int(time.time() * 1000)

        sql = """
        INSERT INTO heartbeats (source, service, ts)
        VALUES (%s, %s, %s)
        ON CONFLICT (source, service, ts) DO NOTHING;
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (self.source, service, ts_ms))
        self.conn.commit()

    def latest(self, service: str) -> Optional[int]:
        sql = """
        SELECT ts
        FROM heartbeats
        WHERE source = %s AND service = %s
        ORDER BY ts DESC
        LIMIT 1;
        """
        with self.conn.cursor() as cur:
            cur.execute(sql, (self.source, service))
            row = cur.fetchone()
        return int(row[0]) if row else None
