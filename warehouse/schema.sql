CREATE DATABASE warehouse;
CREATE SCHEMA warehouse_coin;
SET search_path TO warehouse_coin;
CREATE TABLE dim_date (
    date_sk        BIGSERIAL PRIMARY KEY,
    full_date      DATE NOT NULL UNIQUE,
    day            SMALLINT NOT NULL,
    month          SMALLINT NOT NULL,
    year           SMALLINT NOT NULL,
    quarter        SMALLINT NOT NULL,
    week_of_year   SMALLINT,
    day_of_week    SMALLINT
);
CREATE TABLE dim_coin (
    coin_sk     BIGSERIAL PRIMARY KEY,
    coin_id     VARCHAR(50) NOT NULL,        -- business key (btc, eth…)
    coin_name   VARCHAR(100) NOT NULL,
    symbol      VARCHAR(20),
    start_date  TIMESTAMP NOT NULL,
    end_date    TIMESTAMP,
    is_active   BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE INDEX idx_dim_coin_bk ON dim_coin (coin_id);
CREATE TABLE dim_source (
    source_sk   BIGSERIAL PRIMARY KEY,
    source_id   VARCHAR(50) NOT NULL UNIQUE,   -- api / website code
    source_name VARCHAR(255),
    source_url  TEXT
);
CREATE TABLE dim_author (
    author_sk   BIGSERIAL PRIMARY KEY,
    author_id   VARCHAR(100) NOT NULL UNIQUE,
    author_name VARCHAR(255)
);
CREATE TABLE fact_market (
    fact_market_sk BIGSERIAL PRIMARY KEY,

    coin_sk     BIGINT NOT NULL,
    date_sk     BIGINT NOT NULL,

    price       NUMERIC(18,8),
    volume      NUMERIC(24,8),
    market_cap  NUMERIC(24,8),

    CONSTRAINT fk_market_coin
        FOREIGN KEY (coin_sk) REFERENCES dim_coin (coin_sk),

    CONSTRAINT fk_market_date
        FOREIGN KEY (date_sk) REFERENCES dim_date (date_sk),

    CONSTRAINT uq_market UNIQUE (coin_sk, date_sk)
);
CREATE TABLE fact_news (
    fact_news_sk BIGSERIAL PRIMARY KEY,

    coin_sk     BIGINT NOT NULL,
    date_sk     BIGINT NOT NULL,
    source_sk   BIGINT NOT NULL,
    author_sk   BIGINT,

    news_count  INTEGER NOT NULL,

    CONSTRAINT fk_news_coin
        FOREIGN KEY (coin_sk) REFERENCES dim_coin (coin_sk),

    CONSTRAINT fk_news_date
        FOREIGN KEY (date_sk) REFERENCES dim_date (date_sk),

    CONSTRAINT fk_news_source
        FOREIGN KEY (source_sk) REFERENCES dim_source (source_sk),

    CONSTRAINT fk_news_author
        FOREIGN KEY (author_sk) REFERENCES dim_author (author_sk),

    CONSTRAINT uq_news UNIQUE (coin_sk, date_sk, source_sk, author_sk)
);
