from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml


DEFAULT_CONFIG_PATHS = [
    "app/config/features.yaml",
    "app/config/features.yml",
]


@dataclass(frozen=True)
class FeatureConfig:
    symbol: str
    timeframes: List[str]
    warmup_default: int
    warmup_overrides: Dict[str, int]
    indicators: Dict[str, Any]
    out_dir: str

    def warmup_for(self, timeframe: str) -> int:
        return int(self.warmup_overrides.get(timeframe, self.warmup_default))


def _deep_get(d: Dict[str, Any], path: List[str], default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict) or k not in cur:
            return default
        cur = cur[k]
    return cur


def load_feature_config(config_path: Optional[str] = None) -> FeatureConfig:
    # pick a path
    if config_path:
        p = Path(config_path)
        if not p.exists():
            raise FileNotFoundError(f"feature config not found: {config_path}")
        use_path = p
    else:
        use_path = None
        for cand in DEFAULT_CONFIG_PATHS:
            p = Path(cand)
            if p.exists():
                use_path = p
                break
        if use_path is None:
            raise FileNotFoundError(
                f"no feature config found. tried: {DEFAULT_CONFIG_PATHS}. "
                f"create config/features.yaml"
            )

    with open(use_path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}

    fc = raw.get("features", {})
    symbol = str(fc.get("symbol", "BTC-USDT-SWAP"))
    timeframes = list(fc.get("timeframes", ["1h"]))

    warmup = fc.get("warmup", {}) or {}
    warmup_default = int(warmup.get("default", 26))
    warmup_overrides = warmup.get("overrides", {}) or {}

    indicators = fc.get("indicators", {}) or {}
    out_dir = str(_deep_get(fc, ["output", "out_dir"], "data/features"))

    return FeatureConfig(
        symbol=symbol,
        timeframes=[str(x) for x in timeframes],
        warmup_default=warmup_default,
        warmup_overrides={str(k): int(v) for k, v in warmup_overrides.items()},
        indicators=indicators,
        out_dir=out_dir,
    )
