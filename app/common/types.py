# app/common/types.py
from dataclasses import dataclass

@dataclass(frozen=True)
class Bar:
    symbol: str
    timeframe: str
    ts: int            # 毫秒
    open: float
    high: float
    low: float
    close: float
    volume: float
    source: str = "okx"
