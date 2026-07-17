from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime
from assets import silver_weather_asset

@dag(schedule="@daily", start_date=datetime(2026, 7, 10), catchup=False)
def oceanix_weather_pipeline():
    
    ingest_weather_bronze = BashOperator(
        task_id="extract_weather_api",
        bash_command="python /opt/airflow/src/ingest_weather_bronze.py --date 2026-07-10"
    )

    transform_weather_silver = BashOperator(
        task_id="transform_weather_silver",
        bash_command="python /opt/airflow/src/transform_weather_silver.py --date 2026-07-10",
        outlets=[silver_weather_asset]
    )

    ingest_weather_bronze >> transform_weather_silver

oceanix_weather_pipeline()