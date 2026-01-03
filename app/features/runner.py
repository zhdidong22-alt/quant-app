import argparse
from pathlib import Path

from app.features.config import load_feature_config
from app.features.service import compute_features
from app.features.storage import save_features_csv
from app.storage.db import load_config
from app.storage.bar_repo import BarRepository


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="app/config/features.yaml")
    ap.add_argument("--source", choices=["db", "csv"], default="db")
    ap.add_argument("--symbol", default=None)
    ap.add_argument("--timeframe", default=None)
    ap.add_argument("--start_ts", type=int, default=None)
    ap.add_argument("--end_ts", type=int, default=None)
    ap.add_argument("--limit", type=int, default=None)
    ap.add_argument("--bars_csv", default=None)
    ap.add_argument("--out_csv", default=None)
    args = ap.parse_args()

    fcfg = load_feature_config(args.config)
    symbol = args.symbol or fcfg.symbol
    timeframe = args.timeframe or (fcfg.timeframes[0] if fcfg.timeframes else "1h")
    warmup = fcfg.warmup_for(timeframe)
    indicators = fcfg.indicators

    if args.source == "csv":
        if not args.bars_csv:
            raise ValueError("--bars_csv is required when --source=csv")
        import pandas as pd
        df = pd.read_csv(args.bars_csv)
    else:
        cfg = load_config()
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

    df_feat = compute_features(df, indicator_cfg=indicators, warmup=warmup)

    out_csv = args.out_csv
    if not out_csv:
        out_dir = Path(fcfg.out_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        out_csv = str(out_dir / f"{symbol}_{timeframe}.features.csv")
    else:
        Path(out_csv).parent.mkdir(parents=True, exist_ok=True)

    save_features_csv(df_feat, out_csv)

    print(
        f"[feature] source={args.source} symbol={symbol} timeframe={timeframe} "
        f"input_rows={len(df)} warmup={warmup} output_rows={len(df_feat)} out={out_csv}"
    )


if __name__ == "__main__":
    main()
