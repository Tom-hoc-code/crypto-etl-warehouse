# build_dimensions.py

from pyspark.sql.functions import *
from pyspark.sql.window import Window

from utils.spark_session import get_spark_session


# =========================
# SPARK
# =========================
spark = get_spark_session("Build Dimensions")

STAGING = "/app/data/staging"

JDBC_URL = "jdbc:postgresql://postgres-dw:5432/warehouse"

PROPS = {
    "user": "dw",
    "password": "dw",
    "driver": "org.postgresql.Driver"
}


# =========================
# DIM COIN
# =========================
def build_dim_coin():

    df = spark.read.parquet(f"{STAGING}/coins")

    dim = (
        df
        .select(
            "coin_id",
            "name",
            "symbol"
        )
        .dropDuplicates(["coin_id"])

        .withColumn(
            "coin_sk",
            row_number().over(
                Window.orderBy("coin_id")
            )
        )

        .withColumn(
            "start_date",
            current_timestamp()
        )

        .withColumn(
            "end_date",
            lit(None).cast("timestamp")
        )

        .withColumn(
            "is_active",
            lit(True)
        )

        .select(
            "coin_sk",
            "coin_id",
            "name",
            "symbol",
            "start_date",
            "end_date",
            "is_active"
        )
    )

    (
        dim.write
        .jdbc(
            url=JDBC_URL,
            table="dim_coin",
            mode="overwrite",
            properties=PROPS
        )
    )


# =========================
# DIM DATE
# =========================
def build_dim_date():

    coins = (
        spark.read.parquet(f"{STAGING}/coins")
        .select("date")
    )

    news = (
        spark.read.parquet(f"{STAGING}/news")
        .select("date")
    )

    dates = (
        coins
        .union(news)
        .dropDuplicates()
    )

    dim = (
        dates

        .withColumn(
            "date_sk",
            row_number().over(
                Window.orderBy("date")
            )
        )

        .withColumn(
            "day",
            dayofmonth("date")
        )

        .withColumn(
            "month",
            month("date")
        )

        .withColumn(
            "year",
            year("date")
        )

        .withColumn(
            "quarter",
            quarter("date")
        )

        .withColumn(
            "weekday",
            date_format("date", "E")
        )
    )

    (
        dim.write
        .jdbc(
            url=JDBC_URL,
            table="dim_date",
            mode="overwrite",
            properties=PROPS
        )
    )


# =========================
# DIM SOURCE
# =========================
def build_dim_source():

    dim = (
        spark.read.parquet(f"{STAGING}/news")

        .select("source")

        .dropDuplicates()

        .withColumn(
            "source_sk",
            row_number().over(
                Window.orderBy("source")
            )
        )

        .select(
            "source_sk",
            "source"
        )
    )

    (
        dim.write
        .jdbc(
            url=JDBC_URL,
            table="dim_source",
            mode="overwrite",
            properties=PROPS
        )
    )


# =========================
# MAIN
# =========================
if __name__ == "__main__":

    build_dim_coin()
    build_dim_date()
    build_dim_source()

    spark.stop()