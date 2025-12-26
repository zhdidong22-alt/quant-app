from __future__ import annotations

from app.storage.db import load_config, make_conn, migrate
from app.storage.bar_repo import BarRepository

from app.market.client import OKXMarketClient
from app.market.normalizer import OKXNormalizer
from app.market.fetcher import MarketFetcher

MS_1H = 3600000


def find_gaps(conn, symbol: str, timeframe: str):
    sql = """
    SELECT ts
    FROM bars
    WHERE symbol=%s AND timeframe=%s
    ORDER BY ts ASC;
    """
    with conn.cursor() as cur:
        cur.execute(sql, (symbol, timeframe))
        ts_list = [int(r[0]) for r in cur.fetchall()]

    gaps = []
    for a, b in zip(ts_list, ts_list[1:]):
        diff = b - a
        if diff > MS_1H:
            gaps.append((a, b, diff))
    return gaps


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
    batch_limit = int(data_cfg.get("limit", 100))

    client = OKXMarketClient(base_url=base_url)
    normalizer = OKXNormalizer(ts_unit=data_cfg.get("ts_unit", "ms"))
    fetcher = MarketFetcher(client, normalizer)

    gaps = find_gaps(conn, symbol, timeframe)
    print(f"[fill] symbol={symbol} timeframe={timeframe} gaps={len(gaps)}")

    # 安全阈值：最多修 50 个缺口，防止误操作跑太久
    gaps = gaps[:50]

    for idx, (prev_ts, next_ts, diff) in enumerate(gaps, 1):
        missing = diff // MS_1H - 1
        print(f"[fill] {idx:02d} prev={prev_ts} next={next_ts} missing={missing}")

        # 我们用 after=next_ts 去拿“早于 next_ts 的数据”，
        # 然后写入库里，直到 prev_ts 与 next_ts 之间补齐。
        after_ts = next_ts
        safety_rounds = 0

        while safety_rounds < 50:
            safety_rounds += 1
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
                print("  [fill] no data returned; stop this gap.")
                break

            repo.upsert_many(bars)

            # 更新 after_ts 继续向过去
            after_ts = bars[0].ts

            # 判断缺口是否已补齐：库里 prev_ts 和 next_ts 之间是否连续
            # 轻量做法：只检查“prev_ts + 1H”是否已存在（逐步推进）
            check_ts = prev_ts + MS_1H
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT 1 FROM bars WHERE symbol=%s AND timeframe=%s AND ts=%s LIMIT 1;",
                    (symbol, timeframe, check_ts),
                )
                ok = cur.fetchone() is not None

            print(f"  [fill] round={safety_rounds} wrote={len(bars)} next_after={after_ts} first_missing_exists={ok}")

            if ok:
                # 这只是确认缺口开始处有了；完整性再用 check_gaps 复核
                break

    conn.close()
    print("[fill] done; run check_gaps to verify.")


if __name__ == "__main__":
    main()
