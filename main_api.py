import asyncio
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, status, Query

import uvicorn
import logging
from database import postgres, clickhouse
from schemes import Types, Coin
from schemes.coin import ExtendedCoin
from services.api import get_extended_coin

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


# Инициализация FastAPI
@asynccontextmanager
async def lifespan(app: FastAPI):
    await asyncio.sleep(2)
    await clickhouse.connect()
    await postgres.connect()
    logger.info("clickhouse connection established")
    yield


app = FastAPI(title="Public API Proxy Service", lifespan=lifespan)


@app.get("/api/tickers")
async def get_tickers(
        market: Optional[str] = Query(None, description="Наименование биржи"),
        symbol: Optional[str] = Query(None, description="Имя пары (UPPERCASE)"),
        type: Optional[Types] = Query(None, description="Тип")
):
    base_sql = """
        SELECT m.name AS market_name, symbol, type, price, spread, index_price, volume_24h,  open_interest, funding_rate, ts
        FROM coins c
        RIGHT JOIN markets m ON m.id = c.market_id
    """

    conditions = []
    if market:
        conditions.append(f"m.name='{market}'")
    if symbol:
        conditions.append(f"symbol='{symbol}'")
    if type:
        conditions.append(f"type='{type}'")

    if conditions:
        base_sql += " WHERE " + " AND ".join(conditions)

    try:
        return await postgres.fetch(base_sql)
    except Exception as e:
        logging.error(f"Внутренняя ошибка сервиса: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Внутренняя ошибка сервиса")


@app.get("/api/tickers/history")
async def get_tickers_history(
        market: Optional[str] = Query(None, description="Наименование биржи"),
        symbol: Optional[str] = Query(None, description="Имя пары (UPPERCASE)"),
        type: Optional[Types] = Query(None, description="Тип"),
        limit: Optional[int] = Query(10, description="Тип"),
) -> list[Coin]:
    base_sql = """
            SELECT market_name, symbol, type, price, spread, index_price, volume_24h, open_interest, funding_rate, ts
            FROM coins c
        """

    conditions = []
    if market:
        conditions.append(f"market_name='{market}'")
    if symbol:
        conditions.append(f"symbol='{symbol}'")
    if type:
        conditions.append(f"type='{type}'")

    if conditions:
        base_sql += " WHERE " + " AND ".join(conditions)
    base_sql += f"\n LIMIT {limit}"
    try:
        res = await clickhouse.execute(base_sql)
        coins = []
        for row in res.result_rows:
            coins.append(Coin(
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
        return coins
    except Exception as e:
        logging.error(f"Внутренняя ошибка сервиса: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Внутренняя ошибка сервиса")


@app.get("/api/volume_by_market_pair")
async def get_volume_by_market_pairt(
        market: Optional[str] = Query('bybit', description="Наименование биржи"),
        limit: Optional[int] = Query(10),
        type: Types = Query(Types.spot)
):
    base_sql = f"""
            SELECT
                m.name AS market_name,
                c.symbol,
                c.type,
                (c.volume_24h / COALESCE(sm.sum_volume, 1)) * 100 AS perc_volume
            FROM
                markets m
            LEFT JOIN
                coins c ON m.id = c.market_id
            LEFT JOIN
                (SELECT SUM(volume_24h) AS sum_volume
                 FROM coins c
                 RIGHT JOIN markets m ON m.id = c.market_id
                 WHERE m.name = '{market}') sm ON TRUE
            where m.name='{market}' and c.type='{type}'
            GROUP BY
                m.name, c.symbol, c.type, c.volume_24h, sm.sum_volume
            order BY perc_volume desc
            LIMIT {limit}
        """
    try:
        return await postgres.fetch(base_sql)
    except Exception as e:
        logging.error(f"Внутренняя ошибка сервиса: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Внутренняя ошибка сервиса")


@app.get('/api/extended_tickers')
async def def_extended_coins(
        market: Optional[str] = Query('bybit', description="Наименование биржи"),
        symbol: Optional[str] = Query('BTCUSDT', description="Имя пары (UPPERCASE)"),
        type: Optional[Types] = Query('linear', description="Тип"),
        limit: Optional[int] = Query(10, description="Лимит на кол-во строк")
) -> list[ExtendedCoin]:
    return await get_extended_coin(market, symbol, type, limit)


if __name__ == "__main__":
    uvicorn.run("main_api:app", host="0.0.0.0", port=8000, reload=True)
