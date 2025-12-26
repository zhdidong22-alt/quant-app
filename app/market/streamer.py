import time

class IncrementalStreamer:
    def __init__(self, client, repo, poll_seconds=60, on_heartbeat=None):
        self.client = client
        self.repo = repo
        self.poll_seconds = poll_seconds
        self.on_heartbeat = on_heartbeat

    def _heartbeat(self):
        if self.on_heartbeat:
            self.on_heartbeat()

    def run(self, symbol, timeframe, bar, limit=100):
        print(f"[stream] start symbol={symbol} timeframe={timeframe}")
        while True:
            try:
                bars = self.client.fetch_ohlcv(
                    symbol=symbol,
                    timeframe=timeframe,
                    limit=limit,
                )
                n, latest = self.repo.upsert_bars(symbol, bar, bars)
                self._heartbeat()
                print(f"[stream] upsert={n} latest_ts={latest}")
            except Exception as e:
                print(f"[stream] ERROR {type(e).__name__}: {e}")
                time.sleep(max(self.poll_seconds, 10))
                continue

            time.sleep(self.poll_seconds)
