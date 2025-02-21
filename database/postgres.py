from typing import Final, Any

from psqlpy import ConnectionPool, QueryResult
from config import config


class Postgres:
    _connection_pool: ConnectionPool

    def __init__(self):
        self._connection_pool = ConnectionPool(username=config.POSTGRES_USER,
                                               password=config.POSTGRES_PASS,
                                               host=config.POSTGRES_HOST,
                                               port=config.POSTGRES_PORT,
                                               db_name=config.POSTGRES_DB)

    async def execute(self, sql, params=None) -> Any:
        if params and not isinstance(params, list):
            params = params
        async with self._connection_pool.acquire() as conn:
            results: Final[QueryResult] = await conn.execute(sql, params)
            return results.result()

    async def executemany(self, sql, params: list[list[Any] | Any]) -> Any:
        async with self._connection_pool.acquire() as conn:
            await conn.execute_many(sql, params)


postgres = Postgres()
