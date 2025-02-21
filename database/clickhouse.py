from config import config

from asynch import connect, Connection, DictCursor


class ClickHouse:
    conn: Connection

    async def connect(self):
        self.conn = await connect(
            host=config.CLICKHOUSE_HOST
        )

    async def create_table(self):
        async with self.conn.cursor() as cursor:
            await cursor.execute("CREATE DATABASE IF NOT EXISTS default")
            await cursor.execute("""
                CREATE TABLE IF NOT EXISTS default.coins
                (
                    symbol        String,
                    market_name   String,
                    type          String,
                    price         Float64,
                    index_price   Nullable(Float64),
                    volume        Float64,
                    spread        Float64,
                    open_interest Float64,
                    funding_rate  Float64,
                    ts            Int64
                )
                    engine = MergeTree ORDER BY ts;
            """)

    async def execute(self, sql, params=None):
        async with self.conn.cursor() as cursor:
            await cursor.execute(sql, params)
            return await cursor.fetchall()

clickhouse = ClickHouse()