import argparse
from datetime import datetime, timezone
import pandas as pd

from app.storage.db import load_config
from app.storage.bar_repo import BarRepository


def floor_4h_utc(ts_ms: int) -> int:
    """Floor timestamp (ms) to 4-hour boundary in UTC."""
    # seconds
    ts_s = ts_ms // 1000
    dt = datetime.fromtimestamp(ts_s, tz=timezone.utc)
    hour = dt.hour - (dt.hour % 4)
    dt0 = dt.replace(hour=hour, minute=0, second=0, microsecond=0)
    return int(dt0.timestamp() * 1000)


def aggregate_1m_to_4h(df_1m: pd.DataFrame) -> pd.DataFrame:
    """
    Input df columns: ts, open, high, low, close, volume (ts ms, ascending)
    Output df columns same, timeframe implied by caller ('4h')
    """
    if df_1m.empty:
        return df_1m

    df = df_1m.sort_values("ts").drop_duplicates(subset=["ts"], keep="last").copy()
    df["bucket_ts"] = df["ts"].astype("int64").apply(floor_4h_utc)

    # groupby bucket_ts and aggregate OHLCV
    g = df.groupby("bucket_ts", sort=True)

    out = pd.DataFrame({
        "ts": g["bucket_ts"].first(),
        "open": g["open"].first(),
        "high": g["high"].max(),
        "low": g["low"].min(),
        "close": g["close"].last(),
        "volume": g["volume"].sum(),
    })

    out = out.sort_values("ts").reset_index(drop=True)
    return out


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--symbol", default="BTC-USDT-SWAP")
    ap.add_argument("--start_ts", type=int, default=None, help="ms inclusive")
    ap.add_argument("--end_ts", type=int, default=None, help="ms inclusive")
    ap.add_argument("--limit", type=int, default=None, help="limit 1m bars fetched")
    ap.add_argument("--dry_run", action="store_true", help="do not write to DB")
    args = ap.parse_args()

    cfg = load_config()
    repo = BarRepository(cfg)

    # fetch 1m bars
    df_1m = repo.fetch_bars_df(
        symbol=args.symbol,
        timeframe="1m",
        start_ts=args.start_ts,
        end_ts=args.end_ts,
        limit=args.limit,
        asc=True,
    )
    if df_1m.empty:
        raise RuntimeError("no 1m bars loaded")

    df_4h = aggregate_1m_to_4h(df_1m)

    print(f"[agg] symbol={args.symbol} 1m_rows={len(df_1m)} -> 4h_rows={len(df_4h)}")
    print(df_4h.head(3).to_string(index=False))
    print(df_4h.tail(3).to_string(index=False))

    if args.dry_run:
        print("[agg] dry_run=True, not writing to DB")
        return

    # write 4h bars back using existing upsert (expects list[list])
    bars = df_4h[["ts", "open", "high", "low", "close", "volume"]].values.tolist()
    n, latest = repo.upsert_bars(args.symbol, "4h", bars)
    print(f"[agg] upserted={n} latest_ts={latest}")


if __name__ == "__main__":
    main()
