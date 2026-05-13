from pyspark.sql.functions import (
    col,
    lower,
    trim,
    to_timestamp
)

from utils.spark_session import get_spark_session


# =========================
# SPARK SESSION
# =========================
spark = get_spark_session("Transform Raw")

RAW = "/app/data/raw"
STAGING = "/app/data/staging"


# =========================
# LOAD SYMBOLS
# =========================
def load_symbols():
    return (
        spark.read.csv(
            f"{RAW}/symbols.txt",
            header=False
        )
        .toDF("coin_id", "coin_name")
        .withColumn(
            "coin_id",
            lower(trim(col("coin_id")))
        )
        .withColumn(
            "coin_name",
            lower(trim(col("coin_name")))
        )
        .dropDuplicates(["coin_id"])
    )


# =========================
# COINS SNAPSHOT
# =========================
def transform_coins():

    df = spark.read.json(f"{RAW}/coins")

    df_clean = (
        df.select(

            # identity
            lower(trim(col("symbol"))).alias("coin_id"),
            trim(col("name")).alias("name"),
            lower(trim(col("symbol"))).alias("symbol"),

            # market metrics
            col("price").cast("double"),

            col("fully_diluted_valuation")
                .alias("market_cap")
                .cast("double"),

            col("total_volume")
                .alias("volume")
                .cast("double"),

            col("high_24h").cast("double"),
            col("low_24h").cast("double"),

            col("circulating_supply")
                .cast("double"),

            col("total_supply")
                .cast("double"),

            col("max_supply")
                .cast("double"),

            col("ath").cast("double"),

            # timestamp
            to_timestamp(
                col("time_key")
            ).alias("timestamp")
        )

        .dropna(
            subset=[
                "coin_id",
                "name",
                "timestamp"
            ]
        )

        .filter(col("price") >= 0)

        .dropDuplicates([
            "coin_id",
            "timestamp"
        ])

        .withColumn(
            "date",
            col("timestamp").cast("date")
        )
    )

    (
        df_clean.write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(f"{STAGING}/coins")
    )


# =========================
# NEWS
# =========================
def transform_news():

    df = spark.read.json(f"{RAW}/news")

    symbols = load_symbols()

    # normalize title
    df = df.withColumn(
        "text",
        lower(trim(col("title")))
    )

    # map coin_id from title
    df_joined = (
        df.crossJoin(symbols)
        .filter(
            col("text").contains(
                col("coin_name")
            )
        )
    )

    df_clean = (
        df_joined.select(
            col("coin_id"),

            trim(col("title"))
                .alias("title"),

            trim(col("domain"))
                .alias("source"),

            to_timestamp(
                col("published")
            ).alias("published_at")
        )

        .dropna(
            subset=[
                "coin_id",
                "title",
                "published_at"
            ]
        )

        .dropDuplicates([
            "coin_id",
            "title",
            "published_at"
        ])

        .withColumn(
            "date",
            col("published_at")
            .cast("date")
        )
    )

    (
        df_clean.write
        .mode("overwrite")
        .partitionBy("date")
        .parquet(f"{STAGING}/news")
    )


# =========================
# MAIN
# =========================
if __name__ == "__main__":

    transform_coins()
    transform_news()

    spark.stop()