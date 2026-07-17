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
from dotenv import load_dotenv
from extractors.adls_extractor import ADLSExtractor
from loaders.adls_loader import ADLSLoader

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_silver(target_date_str: str):
    logging.info(f"Iniciando Transformação Silver para a data: {target_date_str}")
    
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    ano, mes, dia = target_date.strftime('%Y'), target_date.strftime('%m'), target_date.strftime('%d')
    particao = f"year={ano}/month={mes}/day={dia}"
    
    # 1. Download da Bronze para disco local 
    remote_bronze_file = f"gfw/events/{particao}/vessel_events_brazil.json"
    local_tmp_file = f"/tmp/bronze_{ano}{mes}{dia}.json"
    
    extractor = ADLSExtractor()
    try:
        extractor.download_file_to_local("bronze", remote_bronze_file, local_tmp_file)
    except Exception:
        logging.warning("Encerrando pipeline devido a falha na extração Silver.")
        return

    # 2. Inicializa o Apache Spark Local
    spark = SparkSession.builder.master("local[1]").appName("Oceanix_Medallion").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    logging.info("Processando Camada Silver (Flattening)...")
    df_bronze = spark.read.json(f"file:{local_tmp_file}")
    
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
    
    caminho_silver_lake = f"gfw/events_flattened/{particao}"
    
    loader = ADLSLoader()
    loader.upload_data_to_container(
        data=buffer_silver.getvalue(),
        container="silver",
        path=caminho_silver_lake,
        file_name="events_flattened.parquet"
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Data de execução (YYYY-MM-DD)")
    args = parser.parse_args()
    process_silver(args.date)