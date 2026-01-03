import argparse
from pathlib import Path

from app.features.service import compute_features
from app.features.storage import save_features_csv
from app.storage.db import load_config
from app.storage.bar_repo import BarRepository


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", choices=["db", "csv"], default="db")
    ap.add_argument("--symbol", default=None)
    ap.add_argument("--timeframe", default=None)
    ap.add_argument("--start_ts", type=int, default=None, help="ms, inclusive")
    ap.add_argument("--end_ts", type=int, default=None, help="ms, inclusive")
    ap.add_argument("--limit", type=int, default=None, help="optional row limit")
    ap.add_argument("--bars_csv", default=None, help="used when --source=csv")
    ap.add_argument("--out_csv", default=None, help="output features csv path")
    args = ap.parse_args()

    cfg = load_config()
    data_cfg = cfg.get("data", {})

    symbol = args.symbol or data_cfg.get("symbol", "BTC-USDT-SWAP")
    timeframe = args.timeframe or data_cfg.get("timeframe", "1h")

    if args.source == "csv":
        if not args.bars_csv:
            raise ValueError("--bars_csv is required when --source=csv")
        import pandas as pd
        df = pd.read_csv(args.bars_csv)
    else:
        repo = BarRepository(cfg)
        df = repo.fetch_bars_df(
            symbol=symbol,
            timeframe=timeframe,
            start_ts=args.start_ts,
            end_ts=args.end_ts,
            limit=args.limit,
            asc=True,
        )

    if df.empty:
        raise RuntimeError(f"no bars loaded. source={args.source} symbol={symbol} timeframe={timeframe}")

    df_feat = compute_features(df)

    out_csv = args.out_csv
    if not out_csv:
        out_csv = f"data/features/{symbol}_{timeframe}.features.csv"

    # ensure path exists
    Path(out_csv).parent.mkdir(parents=True, exist_ok=True)
    save_features_csv(df_feat, out_csv)

    print(f"[feature] source={args.source} symbol={symbol} timeframe={timeframe} "
          f"input_rows={len(df)} warmup=26 output_rows={len(df_feat)} out={out_csv}")


if __name__ == "__main__":
    main()
