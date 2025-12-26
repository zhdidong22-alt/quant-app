from __future__ import annotations

from app.storage.db import load_config, make_conn, migrate
from app.storage.bar_repo import BarRepository

from app.market.client import OKXMarketClient
from app.market.normalizer import OKXNormalizer
from app.market.fetcher import MarketFetcher


def main():
    cfg = load_config("app/config/local.yaml")

    conn = make_conn(cfg)
    migrate(conn)
    repo = BarRepository(conn)

    market_cfg = cfg.get("market", {})
    base_url = market_cfg.get("base_url", "https://www.okx.com")

    data_cfg = cfg.get("data", {})
    symbol = data_cfg.get("symbol", "BTC-USDT-SWAP")
    timeframe = data_cfg.get("timeframe", "1h")
    bar = data_cfg.get("bar", "1H")

    target_rows = int(data_cfg.get("backfill_target_rows", 5000))
    batch_limit = int(data_cfg.get("limit", 100))

    client = OKXMarketClient(base_url=base_url)
    normalizer = OKXNormalizer(ts_unit=data_cfg.get("ts_unit", "ms"))
    fetcher = MarketFetcher(client, normalizer)

    latest = repo.get_latest_ts(symbol, timeframe)
    total = repo.count(symbol, timeframe)
    print(f"[backfill] start count={total} latest_ts={latest}")

    # 关键：用 after 向过去翻页（返回早于 after_ts 的数据）
    after_ts = latest

    while total < target_rows:
        bars = fetcher.fetch(
            symbol=symbol,
            timeframe=timeframe,
            bar=bar,
            limit=batch_limit,
            after=after_ts,
            history=True,
            confirmed_only=True,
        )

        if not bars:
            print("[backfill] no more data returned, stop.")
            break

        repo.upsert_many(bars)
        total = repo.count(symbol, timeframe)

        # 下一页：用本批次最早 ts 继续向更早翻
        after_ts = bars[0].ts
        print(f"[backfill] fetched={len(bars)} total={total} next_after={after_ts}")

    conn.close()
    print("[backfill] done")


if __name__ == "__main__":
    main()
