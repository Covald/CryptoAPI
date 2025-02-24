create table if not exists markets
(
    id   serial
        constraint markets_pk
            primary key,
    name varchar(256) not null
        constraint markets_pk_2
            unique
);

alter table markets
    owner to postgres;

create table if not exists test
(
    id    serial
        constraint test_pk
            primary key,
    test  integer not null
        constraint test_pk_2
            unique,
    test2 integer not null,
    asd   integer,
    asd_2 varchar(256)
);

alter table test
    owner to postgres;

create table if not exists coin_metas
(
    id          serial
        constraint coin_metas_pk
            primary key,
    coin_id     integer not null,
    plus_depth  integer not null,
    minus_depth integer not null
);

alter table coin_metas
    owner to postgres;

create table if not exists coins
(
    id            serial
        constraint coins_pk
            primary key,
    symbol        varchar(50)      not null,
    market_id     integer          not null
        constraint coins_markets_id_fk
            references markets,
    type          varchar(50)      not null,
    price         double precision not null,
    spread        double precision,
    index_price   double precision,
    volume_24h    double precision not null,
    open_interest double precision,
    funding_rate  double precision,
    ts            timestamp        not null,
    constraint coins_pk_2
        unique (symbol, market_id, type)
);

alter table coins
    owner to postgres;

