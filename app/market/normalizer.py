from __future__ import annotations

from app.common.types import Bar


class OKXNormalizer:
    def __init__(self, ts_unit: str = "ms"):
        if ts_unit not in ("ms",):
            raise ValueError("Only ms is supported for now.")
        self.ts_unit = ts_unit

    def is_confirmed(self, row: list) -> bool:
        # row: [ts,o,h,l,c,vol,volCcy,volCcyQuote,confirm]
        # 有些场景可能没有 confirm 字段，保守起见：没有就当已确认
        if len(row) < 9:
            return True
        return str(row[8]) == "1"

    def to_bar(self, symbol: str, timeframe: str, row: list) -> Bar:
        ts = int(row[0])
        o = float(row[1])
        h = float(row[2])
        l = float(row[3])
        c = float(row[4])
        v = float(row[5])
        return Bar(
            symbol=symbol,
            timeframe=timeframe,
            ts=ts,
            open=o,
            high=h,
            low=l,
            close=c,
            volume=v,
            source="okx",
        )
