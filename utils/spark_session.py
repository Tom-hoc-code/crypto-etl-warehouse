from pyspark.sql import SparkSession

def get_spark_session(app_name: str):
    """
    Tạo SparkSession dùng chung cho toàn bộ project
    """
    spark = (
        SparkSession.builder
        .appName(app_name)
        # Nếu chạy standalone Spark trong Docker
        .master("spark://spark-master:7077")
        # Tối ưu nhẹ cho local / demo
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.driver.memory", "2g")
        .config("spark.executor.memory", "2g")
        .getOrCreate()
    )

    return spark
