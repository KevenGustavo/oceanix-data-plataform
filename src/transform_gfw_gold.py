"""
Transformação Gold: Agregação de métricas de negócio.
Lê a camada Silver (Parquet), agrega métricas e salva na camada Gold (Parquet).
"""
import io
import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, when, round
from dotenv import load_dotenv
from extractors.adls_extractor import ADLSExtractor
from loaders.adls_loader import ADLSLoader


load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_gold(target_date_str: str):
    logging.info(f"Iniciando Transformação Gold para a data: {target_date_str}")
    
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    ano, mes, dia = target_date.strftime('%Y'), target_date.strftime('%m'), target_date.strftime('%d')
    particao = f"year={ano}/month={mes}/day={dia}"
    
    # 1. Download da Silver (Parquet) para disco local
    remote_silver_file = f"gfw/events_flattened/{particao}/events_flattened.parquet"
    local_tmp_file = f"/tmp/silver_{ano}{mes}{dia}.parquet"
    
    extractor = ADLSExtractor()
    try:
        extractor.download_file_to_local("silver", remote_silver_file, local_tmp_file)
    except Exception:
        logging.warning("Encerrando pipeline devido a falha na extração Bronze.")
        return

    # 2. Processamento Gold
    spark = SparkSession.builder.master("local[1]").appName("Oceanix_Gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    logging.info("Processando agregações (Gold Layer)...")
    df_silver = spark.read.parquet(local_tmp_file)
    
    # Regras de Negócio (Agregações)
    df_gold = df_silver.groupBy("vessel_id", "vessel_name", "vessel_type").agg(
        round(sum("port_visit_duration_hrs"), 2).alias("total_port_hrs"),
        round(sum("loitering_duration_hrs"), 2).alias("total_loitering_hrs"),
        count(when(col("gap_is_intentional") == True, True)).alias("intentional_gaps_count")
    ).filter(
        (col("total_port_hrs") > 0) | 
        (col("total_loitering_hrs") > 0) | 
        (col("intentional_gaps_count") > 0)
    )

    # 4. Upload da Gold (Buffer In-Memory)
    df_pandas_gold = df_gold.toPandas()
    df_pandas_gold.attrs.clear()
    
    buffer_gold = io.BytesIO()
    df_pandas_gold.to_parquet(buffer_gold, engine="pyarrow", index=False)
    
    caminho_gold_lake = f"gfw/vessel_metrics/{particao}"
    
    loader = ADLSLoader()
    loader.upload_data_to_container(
        data=buffer_gold.getvalue(),
        container="gold",
        path=caminho_gold_lake,
        file_name="vessel_metrics.parquet"
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Data de execução (YYYY-MM-DD)")
    args = parser.parse_args()
    process_gold(args.date)