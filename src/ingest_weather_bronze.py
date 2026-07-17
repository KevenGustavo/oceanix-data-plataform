"""
Ingestão Bronze - Open-Meteo (Marine Weather API).
Extrai dados históricos de clima, vento e ondas para a Baía de São Marcos (Itaqui).
"""
import json
import logging
import argparse
import requests
from datetime import datetime
from dotenv import load_dotenv
from loaders.adls_loader import ADLSLoader

# Configuração
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

PORTS = {
    "Itaqui": {"lat": -2.55, "lon": -44.40},
    "Suape": {"lat": -8.39, "lon": -34.95},
    "Santos": {"lat": -23.95, "lon": -46.33},
    "Paranagua": {"lat": -25.50, "lon": -48.50},
    "Rio Grande": {"lat": -32.13, "lon": -52.05}
}

def ingest_weather_bronze(target_date_str: str):
    logging.info(f"Iniciando Ingestão Bronze (Weather) para a data: {target_date_str}")
    
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    ano, mes, dia = target_date.strftime('%Y'), target_date.strftime('%m'), target_date.strftime('%d')

    all_weather_data = []

    # 1. Requisição para a API Open-Meteo (Historical Marine API)
    url = "https://marine-api.open-meteo.com/v1/marine"

    for port_name, coords in PORTS.items():
        logging.info(f"Extraindo clima para: {port_name}")
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "start_date": target_date_str,
            "end_date": target_date_str,
            "hourly": "wave_height,wind_speed_10m,ocean_current_velocity",
            "timezone": "UTC"
        }
        response = requests.get("https://marine-api.open-meteo.com/v1/marine", params=params)
        
        if response.status_code == 200:
            data = response.json()
            data["port_name"] = port_name 
            all_weather_data.append(data)
    

    json_bytes = json.dumps(all_weather_data).encode('utf-8')    

    # 2. Definição de caminho e Upload
    caminho_lake_bronze = f"weather/year={ano}/month={mes}/day={dia}"
    nome_arquivo = "coastal_weather.json"
    
    logging.info(f"Iniciando ingestão diária: {len(json_bytes)} eventos.")
    loader = ADLSLoader()
    loader.upload_data_to_container(json_bytes, "bronze", caminho_lake_bronze, nome_arquivo)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestão de clima marítimo diário")
    parser.add_argument("--date", required=True, help="Data de execução (YYYY-MM-DD)")
    args = parser.parse_args()
    
    ingest_weather_bronze(args.date)