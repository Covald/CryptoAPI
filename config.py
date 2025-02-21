from os import getenv

from pydantic import BaseModel


class Config(BaseModel):
    POSTGRES_HOST: str = getenv("POSTGRES_HOST", "127.0.0.1")
    POSTGRES_PORT: int = int(getenv("POSTGRES_PORT", 5432))
    POSTGRES_USER: str = getenv("POSTGRES_USER", "postgres")
    POSTGRES_PASS: str = getenv("POSTGRES_PASSWORD", "postgres")
    POSTGRES_DB: str = getenv("POSTGRES_DATABASE", "default")

    CLICKHOUSE_HOST: str | None = getenv("CLICKHOUSE_HOST","127.0.0.1")
    CLICKHOUSE_PORT: int | None = getenv("CLICKHOUSE_PORT")
    CLICKHOUSE_USER: str | None = getenv("CLICKHOUSE_USER")
    CLICKHOUSE_PASSWORD: str | None = getenv("CLICKHOUSE_PASS")
    CLICKHOUSE_DATABASE: str | None = getenv("CLICKHOUSE_DB")


config = Config()
