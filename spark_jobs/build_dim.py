from pyspark.sql.functions import *
from spark_session import get_spark

spark = get_spark()

CURATED = "data/curated"
DW = "data/warehouse/dim"

# =======================
# DIM COIN (SCD TYPE 2)
# =======================
def build_dim_coin():
    df = spark.read.parquet(f"{CURATED}/coins")

    dim = (
        df
        .withColumn("coin_sk", monotonically_increasing_id())
        .withColumn("start_date", current_timestamp())
        .withColumn("end_date", lit(None).cast("timestamp"))
        .withColumn("is_active", lit(True))
        .select(
            "coin_sk", "coin_id", "name", "symbol",
            "start_date", "end_date", "is_active"
        )
    )

    dim.write.mode("overwrite").parquet(f"{DW}/dim_coin")


# =======================
# DIM DATE
# =======================
def build_dim_date():
    dates = (
        spark.read.parquet(f"{CURATED}/prices")
        .select(col("date").cast("date").alias("date"))
        .union(
            spark.read.parquet(f"{CURATED}/news")
            .select(col("published_date").cast("date").alias("date"))
        )
        .dropDuplicates()
    )

    dim = (
        dates
        .withColumn("date_sk", monotonically_increasing_id())
        .withColumn("day", dayofmonth("date"))
        .withColumn("month", month("date"))
        .withColumn("year", year("date"))
        .withColumn("quarter", quarter("date"))
    )

    dim.write.mode("overwrite").parquet(f"{DW}/dim_date")


# =======================
# DIM SOURCE
# =======================
def build_dim_source():
    dim = (
        spark.read.parquet(f"{CURATED}/news")
        .select("source")
        .dropDuplicates()
        .withColumn("source_sk", monotonically_increasing_id())
    )

    dim.write.mode("overwrite").parquet(f"{DW}/dim_source")


# =======================
# DIM AUTHOR
# =======================
def build_dim_author():
    dim = (
        spark.read.parquet(f"{CURATED}/news")
        .select("author")
        .dropDuplicates()
        .withColumn("author_sk", monotonically_increasing_id())
    )

    dim.write.mode("overwrite").parquet(f"{DW}/dim_author")


if __name__ == "__main__":
    build_dim_coin()
    build_dim_date()
    build_dim_source()
    build_dim_author()
    spark.stop()
