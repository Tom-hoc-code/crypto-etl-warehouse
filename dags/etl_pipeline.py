from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    "owner": "data-engineering",
    "retries": 1
}

with DAG(
    dag_id="crypto_data_pipeline",
    default_args=default_args,
    start_date=days_ago(1),
    schedule_interval="@daily",
    catchup=False,
    tags=["crypto", "spark", "etl"]
) as dag:

    crawl_news = BashOperator(
        task_id="crawl_news",
        bash_command="python crawler/crawl_news.py"
    )

    crawl_coins = BashOperator(
        task_id="crawl_coins",
        bash_command="python crawler/crawl_coins.py"
    )

    transform_raw = BashOperator(
        task_id="transform_raw",
        bash_command="spark-submit spark_jobs/transform.py"
    )

    build_dim = BashOperator(
        task_id="build_dim",
        bash_command="spark-submit spark_jobs/build_dim.py"
    )

    build_fact = BashOperator(
        task_id="build_fact",
        bash_command="spark-submit spark_jobs/build_fact.py"
    )

    load_dw = BashOperator(
        task_id="load_dw",
        bash_command="spark-submit spark_jobs/load_dw.py"
    )

    crawl_news >> crawl_coins >> transform_raw >> build_dim >> build_fact >> load_dw