# incremental_facts.py

from pyspark.sql.functions import *

from utils.spark_session import get_spark_session


# =========================
# SPARK SESSION
# =========================
spark = get_spark_session("Incremental Facts")


# =========================
# PATHS
# =========================
STAGING_COINS = "/app/data/staging/coins"
STAGING_NEWS = "/app/data/staging/news"


# =========================
# POSTGRES CONFIG
# =========================
JDBC_URL = "jdbc:postgresql://postgres-dw:5432/warehouse"

PROPS = {
    "user": "dw",
    "password": "dw",
    "driver": "org.postgresql.Driver"
}


# =========================
# LOAD DIMENSIONS
# =========================
def load_dimensions():

    dim_coin = (
        spark.read
        .jdbc(
            url=JDBC_URL,
            table="dim_coin",
            properties=PROPS
        )
        .filter("is_active = true")
    )

    dim_date = (
        spark.read
        .jdbc(
            url=JDBC_URL,
            table="dim_date",
            properties=PROPS
        )
    )

    dim_source = (
        spark.read
        .jdbc(
            url=JDBC_URL,
            table="dim_source",
            properties=PROPS
        )
    )

    return dim_coin, dim_date, dim_source


# =========================
# GET LAST MARKET DATE
# =========================
def get_last_market_date(dim_date):

    try:

        fact_market = (
            spark.read
            .jdbc(
                url=JDBC_URL,
                table="fact_market",
                properties=PROPS
            )
        )

        if fact_market.rdd.isEmpty():
            return None

        last_date_sk = (
            fact_market
            .agg(
                max("date_sk").alias("max_date_sk")
            )
            .collect()[0]["max_date_sk"]
        )

        if last_date_sk is None:
            return None

        last_date = (
            dim_date
            .filter(
                col("date_sk") == last_date_sk
            )
            .select("date")
            .collect()[0]["date"]
        )

        return last_date

    except Exception as e:

        print(f"Cannot read fact_market: {e}")

        return None


# =========================
# GET LAST NEWS TIMESTAMP
# =========================
def get_last_news_timestamp():

    try:

        fact_news = (
            spark.read
            .jdbc(
                url=JDBC_URL,
                table="fact_news",
                properties=PROPS
            )
        )

        if fact_news.rdd.isEmpty():
            return None

        last_timestamp = (
            fact_news
            .agg(
                max("published_at").alias("max_published_at")
            )
            .collect()[0]["max_published_at"]
        )

        return last_timestamp

    except Exception as e:

        print(f"Cannot read fact_news: {e}")

        return None


# =========================
# INCREMENTAL FACT MARKET
# =========================
def incremental_fact_market(dim_coin, dim_date):

    print("Building fact_market...")

    # load staging
    coins = spark.read.parquet(STAGING_COINS)

    # watermark
    last_date = get_last_market_date(dim_date)

    print(f"Last market date: {last_date}")

    # filter new data
    if last_date:

        coins = (
            coins
            .filter(
                col("date") > lit(last_date)
            )
        )

    # build fact
    fact_market = (

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

        .dropDuplicates([
            "coin_sk",
            "date_sk"
        ])
    )

    # no new rows
    if fact_market.rdd.isEmpty():

        print("No new market data")

        return

    # append
    (
        fact_market.write
        .jdbc(
            url=JDBC_URL,
            table="fact_market",
            mode="append",
            properties=PROPS
        )
    )

    print("fact_market updated")


# =========================
# INCREMENTAL FACT NEWS
# =========================
def incremental_fact_news(
    dim_coin,
    dim_date,
    dim_source
):

    print("Building fact_news...")

    # load staging
    news = spark.read.parquet(STAGING_NEWS)

    # watermark
    last_timestamp = get_last_news_timestamp()

    print(f"Last news timestamp: {last_timestamp}")

    # filter new news
    if last_timestamp:

        news = (
            news
            .filter(
                col("published_at") > lit(last_timestamp)
            )
        )

    # build fact
    fact_news = (

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

            dim_date.date_sk.alias("date_sk"),

            dim_coin.coin_sk.alias("coin_sk"),

            dim_source.source_sk.alias("source_sk"),

            news.published_at.alias("published_at")
        )

        .agg(
            count("*").alias("news_count")
        )
    )

    # no new rows
    if fact_news.rdd.isEmpty():

        print("No new news data")

        return

    # append
    (
        fact_news.write
        .jdbc(
            url=JDBC_URL,
            table="fact_news",
            mode="append",
            properties=PROPS
        )
    )

    print("fact_news updated")


# =========================
# MAIN
# =========================
if __name__ == "__main__":

    # load dimensions
    dim_coin, dim_date, dim_source = load_dimensions()

    # update facts
    incremental_fact_market(
        dim_coin,
        dim_date
    )

    incremental_fact_news(
        dim_coin,
        dim_date,
        dim_source
    )

    spark.stop()