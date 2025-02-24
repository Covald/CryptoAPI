create table if not exists coins
(
    symbol        String,
    market_name   String,
    type          String,
    price         Float64,
    spread        Nullable(Float64),
    index_price   Nullable(Float64),
    volume_24h        Float64,
    open_interest Nullable(Float64),
    funding_rate  Nullable(Float64),
    ts            DATETIME
) engine = MergeTree ORDER BY ts;