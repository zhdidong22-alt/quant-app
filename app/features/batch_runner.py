import argparse
from pathlib import Path

from app.features.config import load_feature_config
from app.features.service import compute_features
from app.features.storage import save_features_csv
from app.storage.db import load_config, make_conn
from app.storage.bar_repo import BarRepository


def main():
    ap = argparse.ArgumentParser(description="Batch feature runner (DB -> CSV, config-driven)")
    ap.add_argument("--config", default="app/config/features.yaml")
    ap.add_argument("--symbol", default=None)
    ap.add_argument("--timeframes", default=None, help="override config, e.g. 1m,1h,4h")
    ap.add_argument("--start_ts", type=int, default=None)
    ap.add_argument("--end_ts", type=int, default=None)
    ap.add_argument("--limit", type=int, default=None)
    args = ap.parse_args()

    # ===== feature config =====
    fcfg = load_feature_config(args.config)
    symbol = args.symbol or fcfg.symbol

    if args.timeframes:
        tfs = [x.strip() for x in args.timeframes.split(",") if x.strip()]
    else:
        tfs = list(fcfg.timeframes)

    # ===== source（先写死，后面你再配置化）=====
    source = "okx"

    # ===== feature 输出目录（改这里）=====
    # data/features/{source}/{symbol}/
    base_out_dir = Path(fcfg.out_dir) / source / symbol
    base_out_dir.mkdir(parents=True, exist_ok=True)

    # ===== DB =====
    cfg = load_config()
    conn = make_conn(cfg)
    repo = BarRepository(conn)

    results = []

    for tf in tfs:
        # ===== 从 DB 读取 bars（必须带 source）=====
        df = repo.fetch_bars_df(
            source=source,
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
            results.append((tf, 0, 0, "SKIP(bars=0)"))
            continue

        warmup = fcfg.warmup_for(tf)

        try:
            df_feat = compute_features(
                df,
                indicator_cfg=fcfg.indicators,
                warmup=warmup,
            )
        except Exception as e:
            print(f"[batch][fail] {tf}: bars={bars_n} warmup={warmup} err={e}")
            results.append((tf, bars_n, 0, f"FAIL({e})"))
            continue

        # ===== 输出文件路径（改这里）=====
        # data/features/okx/BTC-USDT-SWAP/1m.features.csv
        out_path = base_out_dir / f"{tf}.features.csv"
        save_features_csv(df_feat, str(out_path))

        feat_n = len(df_feat)
        print(f"[batch][ok] {tf}: bars={bars_n} warmup={warmup} features={feat_n} -> {out_path}")
        results.append((tf, bars_n, feat_n, "OK"))

    conn.close()

    print("\n=== batch summary ===")
    for tf, b, f, st in results:
        print(f"{tf:>5}  bars={b:<6}  features={f:<6}  {st}")


if __name__ == "__main__":
    main()
