from psycopg2.extras import execute_values
import pandas as pd

from app.storage.db import load_config, make_conn


class BarRepository:
    def __init__(self, cfg=None, source: str = "okx"):
        self.cfg = cfg or load_config()
        self.source = source

    def upsert_bars(self, symbol: str, timeframe: str, bars: list[list]):
        """
        bars: list of [ts_ms, open, high, low, close, volume]
        returns: (count, latest_ts_ms)
        """
        if not bars:
            return 0, None

        rows = []
        latest_ts = None
        for b in bars:
            ts = int(b[0])
            o = float(b[1]); h = float(b[2]); l = float(b[3]); c = float(b[4])
            v = float(b[5]) if len(b) > 5 and b[5] is not None else 0.0
            rows.append((symbol, timeframe, ts, o, h, l, c, v, self.source))
            latest_ts = ts if latest_ts is None or ts > latest_ts else latest_ts

        sql = """
        INSERT INTO bars(symbol, timeframe, ts, open, high, low, close, volume, source)
        VALUES %s
        ON CONFLICT (symbol, timeframe, ts) DO UPDATE
        SET open   = EXCLUDED.open,
            high   = EXCLUDED.high,
            low    = EXCLUDED.low,
            close  = EXCLUDED.close,
            volume = EXCLUDED.volume
        """

        conn = make_conn(self.cfg)
        try:
            with conn.cursor() as cur:
                execute_values(cur, sql, rows, page_size=500)
            conn.commit()
        finally:
            conn.close()

        return len(rows), latest_ts

    def fetch_bars_df(
        self,
        symbol: str,
        timeframe: str,
        start_ts: int | None = None,
        end_ts: int | None = None,
        limit: int | None = None,
        asc: bool = True,
    ) -> pd.DataFrame:
        """
        Read bars from Postgres and return a DataFrame with columns:
        ts, open, high, low, close, volume
        ts is milliseconds (BIGINT), consistent with schema.sql.

        start_ts/end_ts: inclusive bounds in ms
        limit: optional row limit
        asc: order by ts ascending (recommended for feature compute)
        """
        where = ["symbol=%s", "timeframe=%s"]
        params: list = [symbol, timeframe]

        if start_ts is not None:
            where.append("ts >= %s")
            params.append(int(start_ts))
        if end_ts is not None:
            where.append("ts <= %s")
            params.append(int(end_ts))

        order = "ASC" if asc else "DESC"
        sql = f"""
        SELECT ts, open, high, low, close, volume
        FROM bars
        WHERE {" AND ".join(where)}
        ORDER BY ts {order}
        """

        if limit is not None:
            sql += " LIMIT %s"
            params.append(int(limit))

        conn = make_conn(self.cfg)
        try:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall()
        finally:
            conn.close()

        df = pd.DataFrame(rows, columns=["ts", "open", "high", "low", "close", "volume"])
        return df
