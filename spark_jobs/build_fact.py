# build_facts.py

from pyspark.sql.functions import *

from utils.spark_session import get_spark_session


# =========================
# SPARK
# =========================
spark = get_spark_session("Build Facts")

STAGING = "/app/data/staging"

JDBC_URL = "jdbc:postgresql://postgres-dw:5432/warehouse"

PROPS = {
    "user": "dw",
    "password": "dw",
    "driver": "org.postgresql.Driver"
}


# =========================
# LOAD DIMENSIONS
# =========================
dim_coin = (
    spark.read
    .jdbc(
        url=JDBC_URL,
        table="dim_coin",
        properties=PROPS
    )
    .filter("is_active = true")
)

dim_date = spark.read.jdbc(
    url=JDBC_URL,
    table="dim_date",
    properties=PROPS
)

dim_source = spark.read.jdbc(
    url=JDBC_URL,
    table="dim_source",
    properties=PROPS
)


# =========================
# FACT MARKET
# =========================
def build_fact_market():

    coins = spark.read.parquet(f"{STAGING}/coins")

    fact = (
        coins

        .dropDuplicates([
            "coin_id",
            "date"
        ])

        .join(
            dim_coin,
            "coin_id"
        )

        .join(
            dim_date,
            coins.date == dim_date.date
        )

        .select(
            dim_date.date_sk.alias("date_sk"),

            dim_coin.coin_sk.alias("coin_sk"),

            coins.price.alias("price"),

            coins.volume.alias("volume"),

            coins.market_cap.alias("market_cap")
        )
    )

    (
        fact.write
        .jdbc(
            url=JDBC_URL,
            table="fact_market",
            mode="append",
            properties=PROPS
        )
    )


# =========================
# FACT NEWS
# =========================
def build_fact_news():

    news = spark.read.parquet(f"{STAGING}/news")

    fact = (
        news

        .dropDuplicates([
            "coin_id",
            "published_at",
            "title"
        ])

        .join(
            dim_coin,
            "coin_id"
        )

        .join(
            dim_date,
            news.date == dim_date.date
        )

        .join(
            dim_source,
            "source"
        )

        .groupBy(
            dim_date.date_sk,
            dim_coin.coin_sk,
            dim_source.source_sk
        )

        .agg(
            count("*").alias("news_count")
        )
    )

    (
        fact.write
        .jdbc(
            url=JDBC_URL,
            table="fact_news",
            mode="append",
            properties=PROPS
        )
    )


# =========================
# MAIN
# =========================
if __name__ == "__main__":

    build_fact_market()
    build_fact_news()

    spark.stop()