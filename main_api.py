from typing import Optional

from fastapi import FastAPI, HTTPException, status, Query

import uvicorn
import logging
from database import postgres, clickhouse
from schemes import Types

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

# Инициализация FastAPI
app = FastAPI(title="Public API Proxy Service", version="1.0")


@app.get("/api/tickers")
async def get_tickers(
        market: Optional[str] = Query(None, description="Наименование биржи"),
        symbol: Optional[str] = Query(None, description="Имя пары (UPPERCASE)"),
        type: Optional[Types] = Query(None, description="Тип")
):
    base_sql = """
        SELECT m.name AS market_name, symbol, type, price, index_price, volume, spread, open_interest, funding_rate, ts
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
        return await postgres.execute(base_sql)
    except Exception as e:
        logging.error(f"Внутренняя ошибка сервиса: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Внутренняя ошибка сервиса")


@app.get("/api/tickers/history")
async def get_tickers_history(
        market: Optional[str] = Query(None, description="Наименование биржи"),
        symbol: Optional[str] = Query(None, description="Имя пары (UPPERCASE)"),
        type: Optional[Types] = Query(None, description="Тип")
):
    base_sql = """
            SELECT m.name AS market_name, symbol, type, price, index_price, volume, spread, open_interest, funding_rate, ts
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
        return await clickhouse.execute(base_sql)
    except Exception as e:
        logging.error(f"Внутренняя ошибка сервиса: {e}")
        raise HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                            detail="Внутренняя ошибка сервиса")


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
