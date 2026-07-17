"""
Transformação Gold: Agregação de métricas de negócio.
Lê a camada Silver (Parquet), agrega métricas e salva na camada Gold (Parquet).
"""
import os
import io
import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, when, round
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_gold(target_date_str: str):
    logging.info(f"Iniciando Transformação Gold para a data: {target_date_str}")
    
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    ano, mes, dia = target_date.strftime('%Y'), target_date.strftime('%m'), target_date.strftime('%d')
    particao = f"year={ano}/month={mes}/day={dia}"
    
    # 1. Conexão Azure
    credential = ClientSecretCredential(os.getenv("AZURE_TENANT_ID"), os.getenv("AZURE_CLIENT_ID"), os.getenv("AZURE_CLIENT_SECRET"))
    service_client = DataLakeServiceClient(account_url=f"https://{os.getenv('AZURE_STORAGE_ACCOUNT_NAME')}.dfs.core.windows.net", credential=credential)

    # 2. Download da Silver (Parquet) para disco local
    caminho_lake_silver = f"gfw/events_flattened/{particao}/events_flattened.parquet"
    caminho_local_tmp = f"/tmp/silver_{ano}{mes}{dia}.parquet"
    
    try:
        logging.info("Baixando dados da Camada Silver...")
        file_client = service_client.get_file_system_client("silver").get_file_client(caminho_lake_silver)
        with open(caminho_local_tmp, "wb") as f:
            f.write(file_client.download_file().readall())
    except Exception as e:
        logging.error(f"Não foi possível baixar dados da Silver. Erro: {e}")
        return

    # 3. Processamento Gold
    spark = SparkSession.builder.master("local[1]").appName("Oceanix_Gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    logging.info("Processando agregações (Gold Layer)...")
    df_silver = spark.read.parquet(caminho_local_tmp)
    
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
    
    caminho_gold_lake = f"gfw/vessel_metrics/{particao}/vessel_metrics.parquet"
    service_client.get_file_system_client("gold").get_file_client(caminho_gold_lake).upload_data(buffer_gold.getvalue(), overwrite=True)
    
    logging.info(f"Camada Gold salva com sucesso em: {caminho_gold_lake}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Data de execução (YYYY-MM-DD)")
    args = parser.parse_args()
    process_gold(args.date)