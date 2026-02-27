from pyspark.sql.functions import *
from spark_session import get_spark

spark = get_spark()

CURATED = "data/curated"
DW = "data/warehouse"

# =======================
# FACT MARKET
# =======================
def build_fact_market():
    prices = spark.read.parquet(f"{CURATED}/prices")
    dim_coin = spark.read.parquet(f"{DW}/dim/dim_coin").filter("is_active = true")
    dim_date = spark.read.parquet(f"{DW}/dim/dim_date")

    fact = (
        prices
        .join(dim_coin, "coin_id")
        .join(dim_date, prices.date == dim_date.date)
        .select(
            dim_date.date_sk,
            dim_coin.coin_sk,
            prices.price,
            prices.volume,
            prices.market_cap
        )
    )

    fact.write.mode("overwrite").parquet(f"{DW}/fact/fact_market")


# =======================
# FACT NEWS
# =======================
def build_fact_news():
    news = spark.read.parquet(f"{CURATED}/news")
    dim_coin = spark.read.parquet(f"{DW}/dim/dim_coin").filter("is_active = true")
    dim_date = spark.read.parquet(f"{DW}/dim/dim_date")
    dim_source = spark.read.parquet(f"{DW}/dim/dim_source")
    dim_author = spark.read.parquet(f"{DW}/dim/dim_author")

    fact = (
        news
        .join(dim_coin, "coin_id")
        .join(dim_date, news.published_date == dim_date.date)
        .join(dim_source, "source")
        .join(dim_author, "author")
        .groupBy(
            dim_date.date_sk,
            dim_coin.coin_sk,
            dim_source.source_sk,
            dim_author.author_sk
        )
        .agg(count("*").alias("news_count"))
    )

    fact.write.mode("overwrite").parquet(f"{DW}/fact/fact_news")


if __name__ == "__main__":
    build_fact_market()
    build_fact_news()
    spark.stop()
