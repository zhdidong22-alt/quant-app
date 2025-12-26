import time
from app.storage.db import load_config, make_conn

class HeartbeatRepository:
    def __init__(self, cfg=None):
        self.cfg = cfg or load_config()

    def beat(self, service_name: str, ts_ms: int | None = None):
        if ts_ms is None:
            ts_ms = int(time.time() * 1000)

        sql = """
        INSERT INTO heartbeat(service_name, last_seen_ts)
        VALUES (%s, %s)
        ON CONFLICT (service_name) DO UPDATE
        SET last_seen_ts = EXCLUDED.last_seen_ts,
            updated_at   = now()
        """

        conn = make_conn(self.cfg)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, (service_name, int(ts_ms)))
            conn.commit()
        finally:
            conn.close()
