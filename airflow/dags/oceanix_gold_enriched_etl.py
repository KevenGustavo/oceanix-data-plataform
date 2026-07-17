from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime
from assets import silver_gfw_asset, silver_weather_asset

@dag(
    dag_id="oceanix_gold_enriched_etl",
    schedule=[silver_gfw_asset, silver_weather_asset], 
    start_date=datetime(2026, 7, 10),
    catchup=False
)
def oceanix_gold_pipeline():
    
    transform_gold_enriched = BashOperator(
        task_id="transform_gold_enriched",
        bash_command="python /opt/airflow/src/transform_gold_enriched.py --date 2026-07-10"
    )

oceanix_gold_pipeline()