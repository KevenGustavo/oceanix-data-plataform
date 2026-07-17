"""
Ingestão Bronze - Open-Meteo (Marine Weather API).
Extrai dados históricos de clima, vento e ondas para a Baía de São Marcos (Itaqui).
"""
import logging
import argparse
import requests
from datetime import datetime
from dotenv import load_dotenv
from loaders.adls_loader import ADLSLoader

# Configuração
load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Coordenadas da área de espera (Loitering) do Complexo do Itaqui
ITAQUI_LAT = -2.55
ITAQUI_LON = -44.40

def ingest_weather_bronze(target_date_str: str):
    logging.info(f"Iniciando Ingestão Bronze (Weather) para a data: {target_date_str}")
    
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    ano, mes, dia = target_date.strftime('%Y'), target_date.strftime('%m'), target_date.strftime('%d')
    
    # 1. Requisição para a API Open-Meteo (Historical Marine API)
    url = "https://marine-api.open-meteo.com/v1/marine"
    params = {
        "latitude": ITAQUI_LAT,
        "longitude": ITAQUI_LON,
        "start_date": target_date_str,
        "end_date": target_date_str,
        "hourly": "wave_height,wind_speed_10m,ocean_current_velocity",
        "timezone": "UTC"
    }
    
    logging.info(f"Fazendo requisição para Open-Meteo API (Lat: {ITAQUI_LAT}, Lon: {ITAQUI_LON})...")
    response = requests.get(url, params=params)
    
    if response.status_code != 200:
        logging.error(f"Erro na API Open-Meteo: {response.status_code} - {response.text}")
        return
        
    weather_data = response.json()
    
    # Validação simples
    if "hourly" not in weather_data:
        logging.warning("API não retornou a chave 'hourly'. Verifique os parâmetros.")
        return

    # 2. Definição de caminho e Upload
    caminho_lake_bronze = f"weather/year={ano}/month={mes}/day={dia}"
    nome_arquivo = "taqui_weather.json"
    
    logging.info(f"Iniciando ingestão diária: {len(weather_data)} eventos.")
    loader = ADLSLoader()
    loader.upload_data_to_container(weather_data, "bronze", caminho_lake_bronze, nome_arquivo)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestão de clima marítimo diário")
    parser.add_argument("--date", required=True, help="Data de execução (YYYY-MM-DD)")
    args = parser.parse_args()
    
    ingest_weather_bronze(args.date)