"""
Transformação Gold Enriquecida (Join GFW + Open-Meteo).
Lê a Silver dos navios (filtrando eventos perto do Porto do Itaqui) e a Silver do Clima.
Cruza as informações para determinar se o atraso operacional foi influenciado por marés perigosas.
"""
import io
import logging
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_trunc, when

# Importando classes utilitárias
from extractors.adls_extractor import ADLSExtractor
from loaders.adls_loader import ADLSLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def process_gold_enriched(target_date_str: str):
    logging.info(f"Iniciando Transformação Gold Enriquecida para a data: {target_date_str}")
    
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    ano, mes, dia = target_date.strftime('%Y'), target_date.strftime('%m'), target_date.strftime('%d')
    path_partition = f"year={ano}/month={mes}/day={dia}"
    
    extractor = ADLSExtractor()
    
    # 1. Download da Silver (Navios)
    remote_silver_gfw = f"gfw/events_flattened/{path_partition}/events_flattened.parquet"
    local_tmp_gfw = f"/tmp/silver_gfw_{ano}{mes}{dia}.parquet"
    
    # 2. Download da Silver (Clima)
    remote_silver_weather = f"weather_flattened/{path_partition}/coastal_weather.parquet"
    local_tmp_weather = f"/tmp/silver_weather_{ano}{mes}{dia}.parquet"
    
    try:
        logging.info("Baixando Silver GFW (Navios)...")
        extractor.download_file_to_local("silver", remote_silver_gfw, local_tmp_gfw)
        
        logging.info("Baixando Silver Open-Meteo (Clima)...")
        extractor.download_file_to_local("silver", remote_silver_weather, local_tmp_weather)
    except Exception:
        logging.error("Falha ao baixar bases Silver. O processamento GFW e Weather rodaram para esta data?")
        return

    # 3. Inicializa Spark
    spark = SparkSession.builder.master("local[1]").appName("Oceanix_Enriched_Gold").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    # 4. Leitura dos DataFrames
    df_gfw = spark.read.parquet(local_tmp_gfw)
    df_weather = spark.read.parquet(local_tmp_weather)
    
    # 5. Lógica de Enriquecimento
    logging.info("Realizando padronização na tabela...")
        
    df_gfw_coastal = df_gfw.withColumn(
        "join_port_name",
        when(col("port_name").ilike("%itaqui%") | col("port_name").ilike("%são marcos%"), "Itaqui")
        .when(col("port_name").ilike("%suape%"), "Suape")
        .when(col("port_name").ilike("%santos%"), "Santos")
        .when(col("port_name").ilike("%paranaguá%") | col("port_name").ilike("%paranagua%"), "Paranagua")
        .when(col("port_name").ilike("%rio grande%"), "Rio Grande")
        .otherwise("Outros")
    ).filter(
        (col("join_port_name") != "Outros") & 
        (col("loitering_duration_hrs") > 0)
    ).withColumn("event_hour", date_trunc("hour", col("start_timestamp")))

    df_gold_enriched = df_gfw_coastal.join(
        df_weather,
        (df_gfw_coastal.event_hour == df_weather.weather_timestamp) & 
        (df_gfw_coastal.join_port_name == df_weather.port_name),
        "left"
    ).select(
        col("vessel_name"),
        col("join_port_name").alias("port"),
        col("start_timestamp").alias("loitering_start"),
        col("loitering_duration_hrs"),
        col("wave_height_m"),
        col("wind_speed_kmh")
    )    
    
    # 6. Salva em Memória e Upload
    logging.info("Enviando Tabela Master Gold para Azure...")
    df_pandas = df_gold_enriched.toPandas()
    df_pandas.attrs.clear()
    
    buffer_gold = io.BytesIO()
    df_pandas.to_parquet(buffer_gold, engine="pyarrow", index=False, coerce_timestamps='us', allow_truncated_timestamps=True)
    
    loader = ADLSLoader()
    loader.upload_data_to_container(
        data=buffer_gold.getvalue(),
        container="gold",
        path=f"operational_impact/{path_partition}",
        file_name="operational_impact.parquet"
    )
    
    logging.info("Pipeline Enriquecido concluído com sucesso!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", required=True, help="Data de execução (YYYY-MM-DD)")
    args = parser.parse_args()
    process_gold_enriched(args.date)