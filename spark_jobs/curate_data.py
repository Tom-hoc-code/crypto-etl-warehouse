from pyspark.sql.functions import col, lower, to_date
from utils.spark_session import get_spark_session

spark = get_spark_session("Curate Data")

STAGING = "data/staging"
CURATED = "data/curated"

def curate_coins():
    df = spark.read.parquet(f"{STAGING}/coins")

    df_curated = (
        df.select(
            col("id").alias("coin_id"),
            lower(col("name")).alias("coin_name"),
            lower(col("symbol")).alias("symbol"),
            col("rank")
        )
    )

    df_curated.write.mode("overwrite").parquet(f"{CURATED}/coins")

def curate_prices():
    df = spark.read.parquet(f"{STAGING}/prices")

    df_curated = (
        df.select(
            "coin_id",
            to_date("timestamp").alias("date"),
            "price",
            "volume",
            "market_cap"
        )
    )

    df_curated.write.mode("overwrite").parquet(f"{CURATED}/prices")

def curate_news():
    df = spark.read.parquet(f"{STAGING}/news")

    df_curated = (
        df.select(
            "coin_id",
            to_date("published_at").alias("published_date"),
            "title",
            "source"
        )
    )

    df_curated.write.mode("overwrite").parquet(f"{CURATED}/news")

if __name__ == "__main__":
    curate_coins()
    curate_prices()
    curate_news()
    spark.stop()
