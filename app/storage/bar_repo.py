# app/storage/bar_repo.py

from typing import Iterable, Tuple, Optional
from psycopg2.extras import execute_values
import pandas as pd

class BarRepository:
    def __init__(self, conn):
        # 这里不再检查 cfg，而是直接接收 conn
        self.conn = conn

    def upsert_bars(
        self,
        symbol: str,
        timeframe: str,
        rows: Iterable[Tuple],
        source: str = "okx",
    ):
        """
        rows: iterable of tuples
          (ts, open, high, low, close, volume)
        """
        sql = """
        INSERT INTO bars (
            source, symbol, timeframe,
            ts, open, high, low, close, volume
        ) VALUES %s
        ON CONFLICT (source, symbol, timeframe, ts)
        DO UPDATE SET
            open = EXCLUDED.open,
            high = EXCLUDED.high,
            low = EXCLUDED.low,
            close = EXCLUDED.close,
            volume = EXCLUDED.volume,
            inserted_at = now()
        RETURNING ts;
        """

        values = [
            (source, symbol, timeframe, ts, o, h, l, c, v)
            for (ts, o, h, l, c, v) in rows
        ]

        with self.conn.cursor() as cur:
            execute_values(cur, sql, values)
            latest = cur.fetchone()
        self.conn.commit()

        return len(values), (latest[0] if latest else None)

    def fetch_bars_df(
        self,
        source: str,
        symbol: str,
        timeframe: str,
        start_ts: int | None = None,
        end_ts: int | None = None,
        limit: int | None = None,
        asc: bool = True,
    ):
        sql = """
        SELECT
            ts, open, high, low, close, volume
        FROM bars
        WHERE source = %s
          AND symbol = %s
          AND timeframe = %s
        """
        params = [source, symbol, timeframe]

        if start_ts is not None:
            sql += " AND ts >= %s"
            params.append(start_ts)

        if end_ts is not None:
            sql += " AND ts <= %s"
            params.append(end_ts)

        order = "ASC" if asc else "DESC"
        sql += f" ORDER BY ts {order}"

        if limit is not None:
            sql += " LIMIT %s"
            params.append(limit)

        df = pd.read_sql(sql, self.conn, params=params)
        return df