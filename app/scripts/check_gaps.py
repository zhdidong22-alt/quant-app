from __future__ import annotations

import sys
from dataclasses import dataclass
from typing import List, Tuple

from app.storage.db import load_config, make_conn

MS_1H = 3600000


@dataclass(frozen=True)
class Gap:
    prev_ts: int
    next_ts: int
    diff: int

    @property
    def missing_bars(self) -> int:
        # diff=2H => missing 1 bar
        return max(self.diff // MS_1H - 1, 0)


def load_ts(conn, symbol: str, timeframe: str) -> List[int]:
    sql = """
    SELECT ts
    FROM bars
    WHERE symbol=%s AND timeframe=%s
    ORDER BY ts ASC;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (symbol, timeframe))
        return [int(r[0]) for r in cur.fetchall()]


def find_gaps(ts_list: List[int], step_ms: int) -> List[Gap]:
    gaps: List[Gap] = []
    for a, b in zip(ts_list, ts_list[1:]):
        diff = b - a
        if diff != step_ms:
            gaps.append(Gap(prev_ts=a, next_ts=b, diff=diff))
    return gaps


def main() -> int:
    cfg = load_config("app/config/local.yaml")
    conn = make_conn(cfg)

    data_cfg = cfg.get("data", {})
    symbol = data_cfg.get("symbol", "BTC-USDT-SWAP")
    timeframe = data_cfg.get("timeframe", "1h")

    # 目前只做 1h 检查；后续可以按 timeframe 映射步长
    step_ms = MS_1H

    ts_list = load_ts(conn, symbol, timeframe)
    conn.close()

    if len(ts_list) < 2:
        print(f"[gaps] symbol={symbol} timeframe={timeframe} rows={len(ts_list)} gaps=0 (not enough rows)")
        return 0

    gaps = find_gaps(ts_list, step_ms)

    print(f"[gaps] symbol={symbol} timeframe={timeframe} rows={len(ts_list)} gaps={len(gaps)} step_ms={step_ms}")

    # 输出前 20 个异常间隔（缺口或异常重复/乱序）
    for i, g in enumerate(gaps[:20], 1):
        missing = g.missing_bars
        kind = "MISSING" if g.diff > step_ms else "ANOMALY"
        print(f"  {i:02d}. {kind} prev={g.prev_ts} next={g.next_ts} diff={g.diff} missing_bars={missing}")

    # 约定：有任何 gap/anomaly => exit code 1
    return 1 if gaps else 0


if __name__ == "__main__":
    sys.exit(main())
