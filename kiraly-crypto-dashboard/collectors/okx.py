import requests

from collectors.http_utils import DEFAULT_HEADERS, get_usdt_eur_mid_coinbase


class OkxCollector:
    BASE_URL = "https://www.okx.com"

    def __init__(self):
        self.last_quote_mode = "direct"

    def _fetch_symbol(self, inst_id: str) -> tuple[float, float]:
        url = f"{self.BASE_URL}/api/v5/market/books?instId={inst_id}&sz=1"
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=(3, 10))
        response.raise_for_status()
        data = response.json()

        if str(data.get("code")) != "0":
            raise RuntimeError(f"OKX API error for {inst_id}: {data.get('msg', 'unknown error')}")

        rows = data.get("data", [])
        if not rows:
            raise RuntimeError(f"No OKX orderbook data for {inst_id}")

        top = rows[0]
        bids = top.get("bids", [])
        asks = top.get("asks", [])
        if not bids or not asks:
            raise RuntimeError(f"Incomplete OKX orderbook for {inst_id}")

        bid = float(bids[0][0])
        ask = float(asks[0][0])
        return bid, ask

    def fetch_top_of_book(self, symbol: str = "BTC-EUR"):
        if symbol != "BTC-EUR":
            raise ValueError(f"Unsupported symbol for OKX: {symbol}")

        try:
            bid_eur, ask_eur = self._fetch_symbol("BTC-EUR")
            self.last_quote_mode = "direct"
            return bid_eur, ask_eur
        except Exception:
            pass

        bid_usdt, ask_usdt = self._fetch_symbol("BTC-USDT")
        usdt_eur = get_usdt_eur_mid_coinbase()

        bid_eur = float(bid_usdt) * float(usdt_eur)
        ask_eur = float(ask_usdt) * float(usdt_eur)
        self.last_quote_mode = "fallback_btcusdt_usdteur"
        return bid_eur, ask_eur
