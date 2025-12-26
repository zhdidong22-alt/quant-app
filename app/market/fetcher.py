from __future__ import annotations

from typing import List, Optional

from app.common.types import Bar
from app.market.client import OKXMarketClient
from app.market.normalizer import OKXNormalizer


class MarketFetcher:
    def __init__(self, client: OKXMarketClient, normalizer: OKXNormalizer):
        self.client = client
        self.normalizer = normalizer

    def fetch(
        self,
        symbol: str,
        timeframe: str,
        bar: str,
        limit: int = 100,
        after: Optional[int] = None,
        before: Optional[int] = None,
        history: bool = False,
        confirmed_only: bool = True,
    ) -> List[Bar]:
        rows = self.client.fetch_candles(
            inst_id=symbol,
            bar=bar,
            limit=limit,
            after=after,
            before=before,
            history=history,
        )

        if confirmed_only:
            rows = [r for r in rows if self.normalizer.is_confirmed(r)]

        bars = [self.normalizer.to_bar(symbol, timeframe, r) for r in rows]
        bars.sort(key=lambda b: b.ts)
        return bars
