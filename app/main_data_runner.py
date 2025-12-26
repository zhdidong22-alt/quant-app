import time
from app.market.client import OKXMarketClient
from app.market.streamer import IncrementalStreamer
from app.storage.bar_repo import BarRepository
from app.storage.heartbeat_repo import HeartbeatRepository
from app.storage.db import load_config

def main():
    cfg = load_config()
    data_cfg = cfg.get("data", {})
    symbol = data_cfg.get("symbol", "BTC-USDT-SWAP")
    timeframe = data_cfg.get("timeframe", "1h")
    bar = data_cfg.get("bar", "1h")
    service_name = f"main_data_runner:{symbol}:{timeframe}"

    repo = BarRepository(cfg)
    hb = HeartbeatRepository(cfg)
    client = OKXMarketClient()

    streamer = IncrementalStreamer(
        client=client,
        repo=repo,
        poll_seconds=int(data_cfg.get("poll_seconds", 60)),
        on_heartbeat=lambda: hb.beat(service_name),
    )

    try:
        streamer.run(
            symbol=symbol,
            timeframe=timeframe,
            bar=bar,
            limit=int(data_cfg.get("limit", 100)),
        )
    except KeyboardInterrupt:
        print("[runner] KeyboardInterrupt received, exiting gracefully.")

if __name__ == "__main__":
    main()
