import asyncio
import time

import pandas as pd
from datetime import datetime, timedelta
from database import postgres, clickhouse
from schemes import Coin

# Для работы с Google Sheets
import gspread
from gspread_dataframe import get_as_dataframe, set_with_dataframe


# Подключение к БД


async def get_yesterday_ts(type: str):
    res = await clickhouse.execute(f"""
    SELECT
        ts,
        COUNT(*) AS cnt
    FROM coins
    WHERE type = '{type}'
      AND ts BETWEEN (now() - 25 * 3600) AND (now() - 23 * 3600)
    GROUP BY ts
    ORDER BY cnt DESC,
             abs(ts - (now() - 24 * 3600)) ASC
    LIMIT 1
    """)
    return res.first_row[0]


async def get_data(type: str) -> tuple[list, list]:
    ts = await get_yesterday_ts(type)
    pg_coins = []
    # Получение данных из Postgres
    query = f"""
        SELECT m.name AS market_name, symbol, type, price, spread, index_price, volume_24h, open_interest, funding_rate, ts
        FROM coins c
        RIGHT JOIN markets m ON m.id = c.market_id
        WHERE type='{type}'
    """
    for row in (await postgres.fetch(query)):
        pg_coins.append(Coin(
            market_name=row[0],
            symbol=row[1],
            type=row[2],
            price=row[3],
            spread=row[4],
            index_price=row[5],
            volume_24h=row[6],
            open_interest=row[7],
            funding_rate=row[8],
            ts=row[9]
        ))
    ch_coins = []
    # Получение данных из Clickhouse
    query_ch = f"select * from coins where ts = '{ts}' and type = '{type}'"
    for row in (await clickhouse.execute(query_ch)).result_set:
        ch_coins.append(Coin(
            market_name=row[1],
            symbol=row[0],
            type=row[2],
            price=row[3],
            spread=row[4],
            index_price=row[5],
            volume_24h=row[6],
            open_interest=row[7],
            funding_rate=row[8],
            ts=row[9]
        ))
    return pg_coins, ch_coins


def get_perc_price(pg: dict, ch: dict) -> float:
    try:
        return round((pg['price'] - ch['price']) / ch['price'] * 100, 7)
    except Exception:
        return None


def get_volume_perc(pg: dict, ch: dict) -> float:
    try:
        return round((pg['volume_24h'] - ch['volume_24h']) / ch['volume_24h'] * 100, 7)
    except Exception:
        return None


async def get_df(type: str) -> pd.DataFrame:
    pg_coins, ch_coins = await get_data(type)
    ch_dict = {}
    for coin in ch_coins:
        dct = coin.model_dump()
        dct.pop('symbol')
        ch_dict[coin.symbol] = dct
    pg_dict = {}
    for coin in pg_coins:
        dct = coin.model_dump()
        dct.pop('symbol')
        pg_dict[coin.symbol] = dct
    rows = []
    for coin in pg_coins:
        symbol = coin.symbol
        ch_dict_for_symbol = ch_dict.get(symbol, {})
        pg_data = pg_dict.get(symbol)
        if not pg_data:
            continue
        rows.append(pd.Series({
            "market_name": "bybit",
            "type": type,
            "symbol": symbol,
            "price": pg_data["price"],
            "price_perc": get_perc_price(pg_data, ch_dict_for_symbol),
            "spread": pg_data["spread"],
            "volume_24h": pg_data["volume_24h"],
            "volume_24h_perc": get_volume_perc(pg_data, ch_dict_for_symbol),
            "open_interest": pg_data["open_interest"],
            "funding_rate": pg_data["funding_rate"],
            "ts": pg_data["ts"],
        }))
    return pd.DataFrame(rows).sort_values(by='price_perc',ascending=False)


async def main():
    await asyncio.sleep(2)
    await postgres.connect()
    await clickhouse.connect()
    while True:
        ts = time.time()
        # Настройка подключения к Google Sheets через сервисный аккаунт
        gc = gspread.service_account(filename='credentials.json')  # Укажите путь к вашему JSON-файлу
        SPREADSHEET_ID = '1P9gvIxmhX_HJt88D78ByLLGx197UvIRsgE51SYcEwrM'  # Замените на идентификатор вашей таблицы
        sh = gc.open_by_key(SPREADSHEET_ID)

        worksheet_spot = sh.worksheet("spot")
        worksheet_linear = sh.worksheet("linear")

        df_spot, df_linear = await asyncio.gather(*[get_df(t) for t in ('spot', 'linear')])

        set_with_dataframe(worksheet_spot, df_spot)
        set_with_dataframe(worksheet_linear, df_linear)
        await asyncio.sleep(int(60 - (time.time() - ts)))


if __name__ == "__main__":
    asyncio.run(main())
