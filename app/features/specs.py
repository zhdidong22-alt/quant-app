FEATURE_SPECS = {
    "ma": [5, 10, 20],
    "atr": [14],
    "rsi": [14],
    "macd": {
        "fast": 12,
        "slow": 26,
        "signal": 9
    }
}

WARMUP = 26

REQUIRED_COLS = [
    "ts", "open", "high", "low", "close", "volume"
]
