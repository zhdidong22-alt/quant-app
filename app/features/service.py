import pandas as pd
import numpy as np


def _ema(series: pd.Series, span: int) -> pd.Series:
    return series.ewm(span=span, adjust=False).mean()


def _rsi(close: pd.Series, window: int) -> pd.Series:
    delta = close.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(window=window, min_periods=window).mean()
    avg_loss = loss.rolling(window=window, min_periods=window).mean()
    rs = avg_gain / avg_loss
    return 100 - (100 / (1 + rs))


def _atr(high: pd.Series, low: pd.Series, close: pd.Series, window: int) -> pd.Series:
    prev_close = close.shift(1)
    tr = pd.concat([
        (high - low),
        (high - prev_close).abs(),
        (low - prev_close).abs()
    ], axis=1).max(axis=1)
    return tr.rolling(window=window, min_periods=window).mean()


def compute_features(
    df: pd.DataFrame,
    indicator_cfg: dict | None = None,
    warmup: int = 26,
) -> pd.DataFrame:
    """
    Input df columns: ts, open, high, low, close, volume
    Output: df with features, dropping warmup rows.
    """
    if df is None or len(df) == 0:
        raise ValueError("empty bars")

    req = ["ts", "open", "high", "low", "close", "volume"]
    for c in req:
        if c not in df.columns:
            raise ValueError(f"missing column: {c}")

    if len(df) <= warmup:
        raise ValueError(f"not enough rows: {len(df)} <= warmup({warmup})")

    x = df.sort_values("ts").copy()

    ind = indicator_cfg or {}
    close = x["close"].astype(float)
    high = x["high"].astype(float)
    low = x["low"].astype(float)

    # ===== MA =====
    ma_cfg = ind.get("ma", {}) or {}
    ma_windows = ma_cfg.get("windows", [5, 10, 20])
    for w in ma_windows:
        w = int(w)
        x[f"ma_{w}"] = close.rolling(window=w, min_periods=w).mean()

    # ===== ATR =====
    atr_cfg = ind.get("atr", {}) or {}
    atr_windows = atr_cfg.get("windows", [14])
    for w in atr_windows:
        w = int(w)
        x[f"atr_{w}"] = _atr(high, low, close, w)

    # ===== RSI =====
    rsi_cfg = ind.get("rsi", {}) or {}
    rsi_windows = rsi_cfg.get("windows", [14])
    for w in rsi_windows:
        w = int(w)
        x[f"rsi_{w}"] = _rsi(close, w)

    # ===== MACD =====
    macd_cfg = ind.get("macd", {}) or {}
    fast = int(macd_cfg.get("fast", 12))
    slow = int(macd_cfg.get("slow", 26))
    signal = int(macd_cfg.get("signal", 9))

    dif = _ema(close, fast) - _ema(close, slow)
    dea = _ema(dif, signal)
    hist = dif - dea
    x["macd_dif"] = dif
    x["macd_dea"] = dea
    x["macd_hist"] = hist

    # drop warmup rows and any NaN/inf
    x = x.iloc[warmup:].copy()
    x.replace([np.inf, -np.inf], np.nan, inplace=True)
    x.dropna(inplace=True)
    x.reset_index(drop=True, inplace=True)
    return x
