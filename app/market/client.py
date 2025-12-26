import requests

class OKXMarketClient:
    """
    Minimal OKX public market client.

    fetch_ohlcv(symbol, timeframe, limit) -> list[list]:
        returns rows: [ts_ms, open, high, low, close, volume]
    """

    BASE_URL = "https://www.okx.com"

    def __init__(self, timeout=10):
        self.timeout = timeout
        self.session = requests.Session()
        self.session.headers.update({"User-Agent": "quant-app/1.0"})

    def _bar_to_okx(self, timeframe: str) -> str:
        # OKX uses "1H", "5m", etc. It is case-insensitive in practice, but keep canonical.
        tf = timeframe.strip()
        # accept "1h" / "1H"
        if tf.lower().endswith("h"):
            return tf[:-1] + "H"
        if tf.lower().endswith("m"):
            return tf[:-1] + "m"
        if tf.lower().endswith("d"):
            return tf[:-1] + "D"
        if tf.lower().endswith("w"):
            return tf[:-1] + "W"
        if tf.lower().endswith("mo"):
            return tf[:-2] + "M"
        return tf

    def fetch_ohlcv(self, symbol: str, timeframe: str = "1h", limit: int = 100):
        """
        Fetch recent candles from OKX public REST API.

        Parameters
        ----------
        symbol : str
            e.g. "BTC-USDT-SWAP"
        timeframe : str
            e.g. "1h", "5m"
        limit : int
            max number of candles

        Returns
        -------
        list[list]
            [ [ts_ms, open, high, low, close, volume], ... ] sorted ascending by ts
        """
        bar = self._bar_to_okx(timeframe)

        url = f"{self.BASE_URL}/api/v5/market/candles"
        params = {"instId": symbol, "bar": bar, "limit": str(int(limit))}

        r = self.session.get(url, params=params, timeout=self.timeout)
        r.raise_for_status()
        payload = r.json()

        if payload.get("code") not in (None, "0"):
            raise RuntimeError(f"OKX error: code={payload.get('code')} msg={payload.get('msg')}")

        data = payload.get("data", [])
        rows = []
        for item in data:
            # item: [ts, o, h, l, c, vol, volCcy, volCcyQuote, confirm]
            ts = int(float(item[0]))
            o = float(item[1]); h = float(item[2]); l = float(item[3]); c = float(item[4])
            v = float(item[5]) if len(item) > 5 and item[5] is not None else 0.0
            rows.append([ts, o, h, l, c, v])

        # OKX returns newest-first; normalize to ascending
        rows.sort(key=lambda x: x[0])
        return rows
