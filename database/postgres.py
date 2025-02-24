from asyncio import get_running_loop, get_event_loop
from typing import Final, Any
from asyncpg import create_pool, Pool,Connection
from config import config


class Postgres:
    _connection_pool: Pool

    async def connect(self):
        self._connection_pool = await create_pool(user=config.POSTGRES_USER,
                                            password=config.POSTGRES_PASS,
                                            host=config.POSTGRES_HOST,
                                            port=config.POSTGRES_PORT,
                                            database=config.POSTGRES_DB)

    async def fetch(self, sql, params: list | None = None) -> Any:
        if params and not isinstance(params, list):
            params = [params]
        async with self._connection_pool.acquire() as conn:
            async with conn.transaction():
                if params:
                    return await conn.fetch(sql, *params)
                else:
                    return await conn.fetch(sql)

    async def execute(self, sql, params: list | None = None) -> Any:
        if params and not isinstance(params, list):
            params = [params]
        async with self._connection_pool.acquire() as conn:
            async with conn.transaction():
                if params:
                    return await conn.execute(sql, *params)
                else:
                    return await conn.execute(sql)

    async def executemany(self, sql, params: list[list[Any] | Any]) -> Any:
        async with self._connection_pool.acquire() as conn: # type: Connection
            async with conn.transaction():
                await conn.executemany(sql, params)


postgres = Postgres()
