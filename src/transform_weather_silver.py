"""
Transformação Silver - Open-Meteo (Clima).
Utiliza classes modulares (Extractor/Loader) para gerenciar I/O com a Azure.
Faz o explode do JSON para granularidade horária e salva em Parquet.
"""
import io
import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, arrays_zip, to_timestamp, current_timestamp
from extractors.adls_extractor import ADLSExtractor
from loaders.adls_loader import ADLSLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_weather_silver(target_date_str: str):
    logging.info(f"Iniciando Transformação Silver (Weather) para a data: {target_date_str}")
    
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    ano, mes, dia = target_date.strftime('%Y'), target_date.strftime('%m'), target_date.strftime('%d')
    
    path_partition = f"year={ano}/month={mes}/day={dia}"
    remote_bronze_file = f"weather/{path_partition}/itaqui_weather.json"
    local_tmp_file = f"/tmp/bronze_weather_{ano}{mes}{dia}.json"
    
    # 1. Extração - Download da Camada Bronze
    extractor = ADLSExtractor()
    try:
        extractor.download_file_to_local("bronze", remote_bronze_file, local_tmp_file)
    except Exception:
        logging.warning("Encerrando pipeline devido a falha na extração Bronze.")
        return

    # 2. Inicializa Spark
    spark = SparkSession.builder.master("local[1]").appName("Oceanix_Weather_Silver").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    logging.info("Processando dados e achatando arrays temporais...")
    df_bronze = spark.read.json(f"file:{local_tmp_file}", multiLine=True)
    
    # 3. Transformação
    df_exploded = df_bronze.select(
        explode(
            arrays_zip(
                col("hourly.time"),
                col("hourly.wave_height"),
                col("hourly.wind_speed_10m"),
                col("hourly.ocean_current_velocity")
            )
        ).alias("hourly_data")
    )
    
    df_silver = df_exploded.select(
        to_timestamp(col("hourly_data.time"), "yyyy-MM-dd'T'HH:mm").alias("weather_timestamp"),
        col("hourly_data.wave_height").alias("wave_height_m"),
        col("hourly_data.wind_speed_10m").alias("wind_speed_kmh"),
        col("hourly_data.ocean_current_velocity").alias("ocean_current_kmh"),
        current_timestamp().alias("ingested_at")
    ).filter(col("weather_timestamp").isNotNull())

    # 4. Salva em Memória (Buffer)
    df_pandas = df_silver.toPandas()
    df_pandas.attrs.clear()
    
    buffer_silver = io.BytesIO()
    df_pandas.to_parquet(buffer_silver, engine="pyarrow", index=False, coerce_timestamps='us', allow_truncated_timestamps=True)
    
    # 5. Carga - Upload para a camada Silver
    loader = ADLSLoader()
    loader.upload_data_to_container(
        data=buffer_silver.getvalue(),
        container="silver",
        path=f"weather_flattened/{path_partition}",
        file_name="itaqui_weather.parquet"
    )

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Data de execução (YYYY-MM-DD)")
    args = parser.parse_args()
    process_weather_silver(args.date)