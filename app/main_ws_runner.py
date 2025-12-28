import asyncio
import time

from app.market.ws_client import OKXWSClient
from app.storage.bar_repo import BarRepository
from app.storage.db import load_config
from app.storage.heartbeat_repo import HeartbeatRepository

SYMBOL = "BTC-USDT-SWAP"
TIMEFRAME = "1m"  # 存库用的 timeframe


def now_ms() -> int:
    return int(time.time() * 1000)


def candle_to_row(c: dict) -> list:
    # BarRepository.upsert_bars 需要: [ts_ms, open, high, low, close, volume]
    return [int(c["ts"]), float(c["open"]), float(c["high"]), float(c["low"]), float(c["close"]), float(c["volume"])]


async def runner():
    cfg = load_config()
    repo = BarRepository(cfg)
    hb = HeartbeatRepository(cfg)

    service_name = f"main_ws_runner:{SYMBOL}:{TIMEFRAME}"

    state = {
        "last_ts": None,   # 当前形成中的 candle 的 ts（分钟起始 ts）
        "buffer": None,    # 当前形成中的 candle（不断被同 ts 更新）
        "last_beat": 0,    # 心跳节流
    }

    def beat():
        # 心跳每 30 秒一次即可，避免刷库
        if now_ms() - state["last_beat"] >= 30_000:
            hb.beat(service_name, now_ms())
            state["last_beat"] = now_ms()

    def on_candle(c: dict):
        """
        c: ws_client 解析后的 candle dict，至少包含:
           ts/open/high/low/close/volume (字符串或数字都行，这里统一转换)
        规则:
        - 同 ts: 只更新 buffer（形成中）
        - ts 变大: 说明新分钟出现 => 上一根已收盘，落库上一根
        """
        beat()

        ts = int(c["ts"])

        if state["last_ts"] is None:
            state["last_ts"] = ts
            state["buffer"] = c
            print(f"[ws_runner] init ts={ts}", flush=True)
            return

        if ts == state["last_ts"]:
            # 同一分钟内不断更新（形成中的K）
            state["buffer"] = c
            return

        if ts > state["last_ts"]:
            # 新一分钟出现 => 上一根收盘，落库上一根（buffer）
            prev = state["buffer"]
            if prev:
                row = candle_to_row(prev)
                n, latest = repo.upsert_bars(SYMBOL, TIMEFRAME, [row])
                print(f"[ws_runner] close&save upsert={n} closed_ts={row[0]} latest_ts={latest}", flush=True)

            state["last_ts"] = ts
            state["buffer"] = c
            return

        # ts < last_ts：异常乱序（一般不会发生），忽略
        print(f"[ws_runner] WARN out_of_order ts={ts} last_ts={state['last_ts']}", flush=True)

    client = OKXWSClient(
        inst_id=SYMBOL,
        on_candle=on_candle,
        logger=lambda s: print(s, flush=True),
    )

    await client.run()


def main():
    try:
        asyncio.run(runner())
    except KeyboardInterrupt:
        print("[ws_runner] KeyboardInterrupt, exiting.", flush=True)


if __name__ == "__main__":
    main()
