"""
Processamento Spark (Silver e Gold Layer).
Lê a Bronze, achata os JSONs (Silver), agrega métricas (Gold) e salva em Parquet na Azure.
"""
import os
import io
import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, current_timestamp
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_silver(target_date_str: str):
    logging.info(f"Iniciando Transformação Silver para a data: {target_date_str}")
    
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    ano, mes, dia = target_date.strftime('%Y'), target_date.strftime('%m'), target_date.strftime('%d')
    particao = f"year={ano}/month={mes}/day={dia}"
    
    # 1. Conexão Azure
    credential = ClientSecretCredential(os.getenv("AZURE_TENANT_ID"), os.getenv("AZURE_CLIENT_ID"), os.getenv("AZURE_CLIENT_SECRET"))
    service_client = DataLakeServiceClient(account_url=f"https://{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net", credential=credential)

    # 2. Download da Bronze para disco local 
    caminho_lake_bronze = f"gfw/events/{particao}/vessel_events_brazil.json"
    caminho_local_tmp = f"/tmp/bronze_{ano}{mes}{dia}.json"
    
    try:
        logging.info("Baixando dados da Camada Bronze...")
        file_client = service_client.get_file_system_client("bronze").get_file_client(caminho_lake_bronze)
        with open(caminho_local_tmp, "wb") as f:
            f.write(file_client.download_file().readall())
    except Exception as e:
        logging.warning(f"Nenhum arquivo Bronze encontrado para esta data ou erro de rede: {e}")
        return

    # 3. Inicializa o Apache Spark Local
    spark = SparkSession.builder.master("local[1]").appName("Oceanix_Medallion").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    logging.info("Processando Camada Silver (Flattening)...")
    df_bronze = spark.read.json(f"file:{caminho_local_tmp}")
    
    df_silver = df_bronze.select(
        col("id").alias("event_id"),
        col("type").alias("event_type"),
        to_timestamp(col("start")).alias("start_timestamp"),
        to_timestamp(col("end")).alias("end_timestamp"),
        col("vessel.id").alias("vessel_id"),
        col("vessel.name").alias("vessel_name"),
        col("vessel.type").alias("vessel_type"),
        col("vessel.flag").alias("vessel_flag"),
        col("port_visit.durationHrs").alias("port_visit_duration_hrs"),
        col("port_visit.startAnchorage.name").alias("port_name"),
        col("loitering.totalTimeHours").alias("loitering_duration_hrs"),
        col("loitering.averageDistanceFromShoreKm").alias("loitering_dist_shore_km"),
        col("gap.durationHours").alias("gap_duration_hrs"),
        col("gap.intentionalDisabling").alias("gap_is_intentional"),
        current_timestamp().alias("ingested_at")
    ).filter(col("vessel_id").isNotNull())

    # Salva Silver In-Memory
    df_pandas_silver = df_silver.toPandas()
    df_pandas_silver.attrs.clear()

    buffer_silver = io.BytesIO()
    df_pandas_silver.to_parquet(
        buffer_silver, 
        engine="pyarrow", 
        index=False, 
        coerce_timestamps='us', 
        allow_truncated_timestamps=True
    )
    
    caminho_gold_lake = f"gfw/events_flattened/{particao}/events_flattened.parquet"
    service_client.get_file_system_client("silver").get_file_client(caminho_gold_lake).upload_data(buffer_silver.getvalue(), overwrite=True)
   
    logging.info(f"Camada Silver salva com sucesso em: {caminho_gold_lake}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Data de execução (YYYY-MM-DD)")
    args = parser.parse_args()
    process_silver(args.date)