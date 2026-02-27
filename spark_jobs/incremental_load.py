from pyspark.sql.functions import max as spark_max
from utils.spark_session import get_spark_session

spark = get_spark_session("Incremental Load")

FACT = "data/warehouse/fact/fact_price"
CURATED = "data/curated/prices"

def incremental_fact_price():
    fact = spark.read.parquet(FACT)
    curated = spark.read.parquet(CURATED)

    last_date = fact.select(spark_max("date")).collect()[0][0]

    new_data = curated.filter(f"date > '{last_date}'")

    new_data.write.mode("append").parquet(FACT)

if __name__ == "__main__":
    incremental_fact_price()
    spark.stop()
