import argparse
from pathlib import Path

from app.storage.db import load_config
from app.storage.bar_repo import BarRepository
from app.features.service import compute_features
from app.features.storage import save_features_csv


def main():
    ap = argparse.ArgumentParser(description="Batch feature runner (DB -> CSV)")
    ap.add_argument("--symbol", default="BTC-USDT-SWAP")
    ap.add_argument(
        "--timeframes",
        default="1m,15m,30m,1h",
        help="comma separated, e.g. 1m,15m,30m,1h"
    )
    ap.add_argument(
        "--out_dir",
        default="data/features",
        help="output directory for features csv"
    )
    ap.add_argument("--start_ts", type=int, default=None)
    ap.add_argument("--end_ts", type=int, default=None)
    ap.add_argument("--limit", type=int, default=None)

    args = ap.parse_args()

    cfg = load_config()
    repo = BarRepository(cfg)

    symbol = args.symbol
    timeframes = [x.strip() for x in args.timeframes.split(",") if x.strip()]
    out_dir = Path(args.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[batch] symbol={symbol} timeframes={timeframes}")

    results = []

    for tf in timeframes:
        df = repo.fetch_bars_df(
            symbol=symbol,
            timeframe=tf,
            start_ts=args.start_ts,
            end_ts=args.end_ts,
            limit=args.limit,
            asc=True,
        )

        bars_n = len(df)
        if bars_n == 0:
            print(f"[batch][skip] {tf}: bars=0")
            results.append((tf, 0, 0, "SKIP"))
            continue

        try:
            df_feat = compute_features(df)
        except Exception as e:
            print(f"[batch][fail] {tf}: bars={bars_n} err={e}")
            results.append((tf, bars_n, 0, "FAIL"))
            continue

        out_path = out_dir / f"{symbol}_{tf}.features.csv"
        save_features_csv(df_feat, out_path)

        feat_n = len(df_feat)
        print(
            f"[batch][ok] {tf}: bars={bars_n} "
            f"warmup=26 features={feat_n} -> {out_path}"
        )
        results.append((tf, bars_n, feat_n, "OK"))

    print("\n=== batch summary ===")
    for tf, b, f, st in results:
        print(f"{tf:>5}  bars={b:<6}  features={f:<6}  {st}")


if __name__ == "__main__":
    main()
