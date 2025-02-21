import asyncio
import time

from config import config

print(config.model_dump())
from sources.bybit import Bybit

from database.tmp import upload_data


async def update_coins(bybit: Bybit):
    while True:
        st = time.time()
        market_name, coins = await bybit.get()
        await upload_data(market_name, coins)
        await asyncio.sleep(round(60 - (time.time() - st)))


# ToDo
async def update_depth(bybit: Bybit):
    from datetime import datetime
    while True:
        st = time.time()
        print(f"Calc depth - {datetime.now()}")
        await asyncio.sleep(round(3600 - (time.time() - st)))


async def main():
    bybit = Bybit()
    await asyncio.gather(
        update_coins(bybit),
        update_depth(bybit)
    )


if __name__ == "__main__":
    asyncio.run(main())
