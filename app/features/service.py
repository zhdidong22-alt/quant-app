import pandas as pd
import numpy as np
from .specs import FEATURE_SPECS, WARMUP, REQUIRED_COLS


def _ema(s: pd.Series, span: int) -> pd.Series:
    return s.ewm(span=span, adjust=False).mean()


def _rsi_wilder(close: pd.Series, period: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0.0)
    loss = (-delta).clip(lower=0.0)

    # Wilder's smoothing (RMA): EMA with alpha=1/period
    avg_gain = gain.ewm(alpha=1/period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, adjust=False).mean()

    rs = avg_gain / avg_loss.replace(0.0, np.nan)
    rsi = 100.0 - (100.0 / (1.0 + rs))
    return rsi.fillna(0.0)


def _atr_wilder(high: pd.Series, low: pd.Series, close: pd.Series, period: int) -> pd.Series:
    prev_close = close.shift(1)
    tr1 = (high - low).abs()
    tr2 = (high - prev_close).abs()
    tr3 = (low - prev_close).abs()
    tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)

    # Wilder's smoothing (RMA): EMA with alpha=1/period
    atr = tr.ewm(alpha=1/period, adjust=False).mean()
    return atr


def compute_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    输入：包含 ts, open, high, low, close, volume 的 DataFrame（bars）
    输出：原始列 + 指标列（已丢弃 warm-up 行），并保证无 NaN/inf
    """
    # 1) 基础校验
    for c in REQUIRED_COLS:
        if c not in df.columns:
            raise ValueError(f"missing column: {c}")

    # 2) 排序/去重（确保可重算一致性）
    df = df.sort_values("ts").drop_duplicates(subset=["ts"], keep="last").reset_index(drop=True)

    # 3) 计算 MA
    for n in FEATURE_SPECS["ma"]:
        df[f"ma_{n}"] = df["close"].rolling(window=n, min_periods=n).mean()

    # 4) 计算 ATR
    for n in FEATURE_SPECS["atr"]:
        df[f"atr_{n}"] = _atr_wilder(df["high"], df["low"], df["close"], n)

    # 5) 计算 RSI
    for n in FEATURE_SPECS["rsi"]:
        df[f"rsi_{n}"] = _rsi_wilder(df["close"], n)

    # 6) 计算 MACD
    macd_cfg = FEATURE_SPECS["macd"]
    fast = macd_cfg["fast"]
    slow = macd_cfg["slow"]
    signal = macd_cfg["signal"]

    ema_fast = _ema(df["close"], fast)
    ema_slow = _ema(df["close"], slow)
    dif = ema_fast - ema_slow
    dea = _ema(dif, signal)
    hist = dif - dea

    df["macd_dif"] = dif
    df["macd_dea"] = dea
    df["macd_hist"] = hist

    # 7) warm-up 丢弃
    if len(df) <= WARMUP:
        raise ValueError(f"not enough rows: {len(df)} <= warmup({WARMUP})")
    out = df.iloc[WARMUP:].reset_index(drop=True)

    # 8) 校验：不得 NaN/inf
    feature_cols = [c for c in out.columns if c not in REQUIRED_COLS]
    chk = out[feature_cols].replace([np.inf, -np.inf], np.nan)
    na_cols = chk.isna().sum()
    bad = na_cols[na_cols > 0]
    if not bad.empty:
        top = bad.sort_values(ascending=False).head(20).to_dict()
        raise ValueError(f"feature columns contain NaN/inf after warmup. top={top}")

    # 9) 物理合理性（轻量约束）
    # RSI ∈ [0,100]（允许微小浮点误差）
    for n in FEATURE_SPECS["rsi"]:
        r = out[f"rsi_{n}"]
        if ((r < -1e-6) | (r > 100.000001)).any():
            raise ValueError(f"RSI out of range for rsi_{n}")

    # ATR > 0（绝大多数应为正；允许极少数为 0 的边界情况）
    for n in FEATURE_SPECS["atr"]:
        a = out[f"atr_{n}"]
        if (a < -1e-9).any():
            raise ValueError(f"ATR negative for atr_{n}")

    # macd_hist = dif - dea（数值一致性）
    if not np.allclose(out["macd_hist"].values, (out["macd_dif"] - out["macd_dea"]).values, rtol=1e-10, atol=1e-10):
        raise ValueError("macd_hist != macd_dif - macd_dea (numeric check failed)")

    return out
