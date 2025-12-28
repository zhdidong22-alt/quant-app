import asyncio
import json
import time
from typing import Callable, Dict, Optional

import websockets

OKX_PUBLIC_WS = "wss://ws.okx.com:8443/ws/v5/business"


class OKXWSClient:
    """
    Minimal OKX public websocket client.
    Subscribes candle1m and calls on_candle(candle_dict) with normalized fields.
    """

    def __init__(self, inst_id: str, on_candle: Callable[[Dict], None], logger: Optional[Callable[[str], None]] = None):
        self.inst_id = inst_id
        self.on_candle = on_candle
        self.log = logger or (lambda s: None)

        self._stop = False

    def stop(self):
        self._stop = True

    async def run(self):
        while not self._stop:
            try:
                await self._run_once()
            except Exception as e:
                self.log(f"[ws] ERROR {type(e).__name__}: {e}")
                # backoff
                await asyncio.sleep(3)

    async def _run_once(self):
        async with websockets.connect(OKX_PUBLIC_WS, ping_interval=20, ping_timeout=10, close_timeout=5) as ws:
            self.log(f"[ws] connected {OKX_PUBLIC_WS}")

            sub = {
                "op": "subscribe",
                "args": [{"channel": "candle1m", "instId": self.inst_id}],
            }
            await ws.send(json.dumps(sub))
            self.log(f"[ws] subscribed candle1m instId={self.inst_id}")

            while not self._stop:
                raw = await ws.recv()
                msg = json.loads(raw)

                # ignore event acks
                if isinstance(msg, dict) and msg.get("event"):
                    self.log(f"[ws] event={msg.get('event')} full={msg}")
                    continue

                data = msg.get("data") if isinstance(msg, dict) else None
                if not data:
                    continue

                # OKX candle data format: [[ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm], ...]
                # Some fields may vary; we only use first 6.
                arr = data[0]
                try:
                    ts = int(arr[0])
                    o = float(arr[1]); h = float(arr[2]); l = float(arr[3]); c = float(arr[4])
                    v = float(arr[5])
                except Exception:
                    continue

                candle = {
                    "symbol": self.inst_id,
                    "timeframe": "1m",
                    "ts": ts,
                    "open": o,
                    "high": h,
                    "low": l,
                    "close": c,
                    "volume": v,
                    "source": "okx_ws",
                    "recv_ts_ms": int(time.time() * 1000),
                }
                self.on_candle(candle)
