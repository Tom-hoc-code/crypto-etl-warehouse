from utils.spark_session import get_spark_session

spark = get_spark_session("Load DW")

JDBC_URL = "jdbc:postgresql://postgres-dw:5432/warehouse"
PROPS = {
    "user": "dw",
    "password": "dw",
    "driver": "org.postgresql.Driver"
}

def load_table(path, table):
    df = spark.read.parquet(path)
    df.write.jdbc(JDBC_URL, table, mode="overwrite", properties=PROPS)

if __name__ == "__main__":
    load_table("data/warehouse/dim/dim_coin", "dim_coin")
    load_table("data/warehouse/fact/fact_price", "fact_price")
    spark.stop()
