from pyspark.sql.functions import col, to_timestamp
from utils.spark_session import get_spark_session

spark = get_spark_session("Transform Raw")

RAW = "data/raw"
STAGING = "data/staging"

def transform_coins():
    df = spark.read.json(f"{RAW}/coins")

    df_clean = (
        df.select(
            col("id"),
            col("name"),
            col("symbol"),
            col("rank").cast("int")
        )
        .dropna(subset=["id", "symbol"])
        .dropDuplicates(["id"])
    )

    df_clean.write.mode("overwrite").parquet(f"{STAGING}/coins")

def transform_prices():
    df = spark.read.json(f"{RAW}/prices")

    df_clean = (
        df.select(
            col("coin_id"),
            col("price").cast("double"),
            col("volume").cast("double"),
            col("market_cap").cast("double"),
            to_timestamp("timestamp").alias("timestamp")
        )
        .dropna(subset=["coin_id", "timestamp"])
    )

    df_clean.write.mode("overwrite").parquet(f"{STAGING}/prices")

def transform_news():
    df = spark.read.json(f"{RAW}/news")

    df_clean = (
        df.select(
            col("coin_id"),
            col("title"),
            col("source"),
            to_timestamp("published_at").alias("published_at")
        )
        .dropna(subset=["coin_id", "title"])
    )

    df_clean.write.mode("overwrite").parquet(f"{STAGING}/news")

if __name__ == "__main__":
    transform_coins()
    transform_prices()
    transform_news()
    spark.stop()
