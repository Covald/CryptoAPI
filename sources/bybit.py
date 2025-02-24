import asyncio
from unicodedata import category

from schemes.coin import Types
from .interfaces import ExchangeInterface
from schemes import Coin
from aiohttp import ClientSession


def _map(coin_data: dict) -> list[Coin]:
    timestamp = coin_data.get('time')
    category = coin_data['result']['category']
    result = []
    for coin in coin_data['result']['list']:
        ask = coin.get('ask1Price')
        bid = coin.get('bid1Price')
        spread = None
        if ask and bid:
            spread = round((float(ask) - float(bid)) / float(ask) * 100,7)
        tmp = {'market_name': None,
               'symbol': coin['symbol'],
               'type': category,
               'price': coin['lastPrice'],
               'index_price': coin.get('usdIndexPrice', None),
               'volume_24h': coin['volume24h'],
               'spread': spread,
               'open_interest': coin.get('openInterestValue', None) or None,
               'funding_rate': coin.get('fundingRate', None) or None,
               'ts': timestamp}
        result.append(Coin(
            **tmp
        ))
    return result


class Bybit(ExchangeInterface):
    __url__ = "https://api.bybit.com"
    _ticker_url = '/v5/market/tickers'
    _funding_url = '/v5/market/funding/history'
    _open_interest_url = '/v5/market/open-interest'
    __market_name__ = 'bybit'

    def __init__(self):
        self.session = ClientSession(self.__url__)

    async def get(self) -> tuple[str, list[Coin]]:
        result = {}
        tasks = []
        for category in Types:
            tasks.append(self._get_tickers(str(category)))
        cor_results = await asyncio.gather(*tasks)
        cor_results = map(_map, cor_results)
        result = dict(zip(Types, cor_results))

        coins = []
        for i in result.values():
            coins.extend(i)

        return self.__market_name__, coins

    async def _get_tickers(self, category: str = 'spot') -> dict:
        return await self._get(self._ticker_url, params={"category": category})

    async def _get(self, endpoint: str, params: dict | None = None) -> dict:
        async with self.session.get(endpoint, params=params) as response:
            response.raise_for_status()
            return await response.json()
