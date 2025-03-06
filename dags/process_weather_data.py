import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from scripts.extract import extract_weather_data_into_bucket


default_args = {
    'start_date': airflow.utils.dates.days_ago(0),
    'retries': 0
}

dag = DAG(
    "process_weather_data",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    schedule_interval="@daily",
    dagrun_timeout=timedelta(minutes=3)
    )

extract_task = PythonOperator(
    task_id="extract",
    python_callable=extract_weather_data_into_bucket,
    op_args=["5a38894979a32ef8c0ac8db6b3368d81", 5, -78, "metric", "minutely,hourly,daily,alerts", "airqualityproject28", "landing"],
    dag=dag
)

extract_task