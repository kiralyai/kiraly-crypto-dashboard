from collectors.binance import BinanceCollector
from collectors.bitvavo import BitvavoCollector
from collectors.bybit import BybitCollector
from collectors.coinbase import CoinbaseCollector
from collectors.kraken import KrakenCollector
from collectors.okx import OkxCollector

COLLECTOR_REGISTRY = {
    "Bitvavo": BitvavoCollector,
    "Coinbase": CoinbaseCollector,
    "Bybit": BybitCollector,
    "Kraken": KrakenCollector,
    "Binance": BinanceCollector,
    "OKX": OkxCollector,
}


def get_collector(name: str):
    collector_cls = COLLECTOR_REGISTRY.get(name)
    if collector_cls is None:
        raise ValueError(f"No collector registered for exchange: {name}")
    return collector_cls()


def get_supported_exchange_names() -> list[str]:
    return list(COLLECTOR_REGISTRY.keys())


def get_market_data_source_links(
    exchange_name: str,
    symbol: str = "BTC-EUR",
    spread_source: str = "",
) -> list[dict[str, str]]:
    is_fallback = str(spread_source or "").lower().startswith("fallback")

    if exchange_name == "Bitvavo":
        return [{"label": "API source", "url": f"https://api.bitvavo.com/v2/ticker/book?market={symbol}"}]

    if exchange_name == "Coinbase":
        return [
            {
                "label": "API source",
                "url": f"https://api.exchange.coinbase.com/products/{symbol}/ticker",
            }
        ]

    if exchange_name == "Kraken":
        pair = {"BTC-EUR": "XXBTZEUR"}.get(symbol, symbol.replace("-", ""))
        return [
            {
                "label": "API source",
                "url": f"https://api.kraken.com/0/public/Depth?pair={pair}&count=1",
            }
        ]

    if exchange_name == "Binance":
        if is_fallback:
            return [
                {
                    "label": "API source",
                    "url": "https://api.binance.com/api/v3/ticker/bookTicker?symbol=BTCUSDT",
                },
                {
                    "label": "FX reference",
                    "url": "https://api.coinbase.com/v2/exchange-rates?currency=USDT",
                },
            ]
        return [
            {
                "label": "API source",
                "url": "https://api.binance.com/api/v3/ticker/bookTicker?symbol=BTCEUR",
            }
        ]

    if exchange_name == "Bybit":
        if is_fallback:
            return [
                {
                    "label": "API source",
                    "url": "https://api.bybit.com/v5/market/orderbook?category=spot&symbol=BTCUSDT&limit=1",
                },
                {
                    "label": "FX reference",
                    "url": "https://api.coinbase.com/v2/exchange-rates?currency=USDT",
                },
            ]
        return [
            {
                "label": "API source",
                "url": "https://api.bybit.com/v5/market/orderbook?category=spot&symbol=BTCEUR&limit=1",
            }
        ]

    if exchange_name == "OKX":
        if is_fallback:
            return [
                {
                    "label": "API source",
                    "url": "https://www.okx.com/api/v5/market/books?instId=BTC-USDT&sz=1",
                },
                {
                    "label": "FX reference",
                    "url": "https://api.coinbase.com/v2/exchange-rates?currency=USDT",
                },
            ]
        return [
            {
                "label": "API source",
                "url": "https://www.okx.com/api/v5/market/books?instId=BTC-EUR&sz=1",
            }
        ]

    return []
