from typing import Any, Coroutine

from clickhouse_connect.driver.query import QueryResult

from config import config
import clickhouse_connect


class ClickHouse:
    conn: clickhouse_connect.driver.AsyncClient

    async def connect(self):
        self.conn = await clickhouse_connect.get_async_client(
            host=config.CLICKHOUSE_HOST
        )

    async def execute(self, sql, params: None | list[dict] = None) -> QueryResult:
        return await self.conn.query(sql, params)


clickhouse = ClickHouse()
