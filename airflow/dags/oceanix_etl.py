"""
DAG de Ingestão e Processamento - Plataforma Oceanix.
Orquestra o pipeline Medallion (Global Fishing Watch -> Azure Data Lake).
Utiliza a TaskFlow API moderna e a macro nativa de data lógica ({{ ds }}).
"""
from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from pendulum import datetime, duration

# Configurações padrão de tolerância a falhas
default_args = {
    "owner": "data_engineering",
    "retries": 2, # Tenta de novo até 2 vezes se a API do GFW cair
    "retry_delay": duration(minutes=5),
}

@dag(
    dag_id="oceanix_daily_etl",
    schedule="@daily",             # Executa uma vez por dia à meia-noite (UTC)
    start_date=datetime(2026, 7, 10), # Data base de início
    catchup=False,                 # Evita rodar tudo de uma vez caso fique dias desligado
    default_args=default_args,
    tags=["oceanix", "blue_economy", "gfw", "pyspark"],
    doc_md="""### Oceanix Data Platform Pipeline
    Esta DAG extrai eventos de embarcações, grava na Azure (Bronze), 
    e dispara o Apache Spark para construir as camadas Silver e Gold."""
)
def oceanix_pipeline():
    
    ingest_bronze = BashOperator(
        task_id="extract_bronze",
        bash_command="python /opt/airflow/src/ingest_bronze.py --date {{ ds }}"
    )

    transform_silver = BashOperator(
        task_id="transform_silver",
        bash_command="python /opt/airflow/src/transform_silver.py --date {{ ds }}"
    )

    transform_gold = BashOperator(
        task_id="transform_gold",
        bash_command="python /opt/airflow/src/transform_gold.py --date {{ ds }}"
    )

    # Ordem linear de execução
    ingest_bronze >> transform_silver >> transform_gold

# Instancia a DAG
oceanix_pipeline()