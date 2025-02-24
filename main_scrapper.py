import asyncio
import time

from config import config
from database import postgres, clickhouse

print(config.model_dump())
from sources.bybit import Bybit

from database.tmp import upload_data


async def update_coins(bybit: Bybit):
    while True:
        st = time.time()
        market_name, coins = await bybit.get()
        await upload_data(market_name, coins)
        await asyncio.sleep(round(60 - (time.time() - st)))


async def main():
    await asyncio.sleep(10)
    await postgres.connect()
    await clickhouse.connect()
    bybit = Bybit()
    while True:
        ts = time.time()

        await update_coins(bybit)
        await asyncio.sleep(60 - int(time.time()-ts))


if __name__ == "__main__":
    asyncio.run(main())
