"""
Extrator da API do Global Fishing Watch (GFW)
Busca eventos marítimos (Port Visits, Loitering e Gaps) utilizando o método POST
filtrados oficialmente pela Zona Econômica Exclusiva (EEZ) do Brasil (Region ID: 8464).
"""

import os
import json
import logging
from datetime import datetime, timedelta
import requests
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class GFWExtractor:
    def __init__(self):
        self.api_token = os.getenv("GFW_API_TOKEN")
        self.base_url = "https://gateway.api.globalfishingwatch.org/v3"
        self.headers = {
            "Authorization": f"Bearer {self.api_token}",
            "Content-Type": "application/json"
        }
        
    def fetch_events(self, start_date: str, end_date: str) -> list:
        endpoint = f"{self.base_url}/events"
        
        payload = {
            "datasets": [
                "public-global-port-visits-events:latest",
                "public-global-loitering-events:latest",
                "public-global-gaps-events:latest"
            ],
            "startDate": start_date,
            "endDate": end_date,
            "region": {
                "dataset": "public-eez-areas",
                "id": "8464"
            }
        }

        all_events = []
        limit = 5000
        offset = 0
        
        logging.info(f"Extraindo dados da EEZ Brasileira (ID 8464) entre {start_date} e {end_date}...")
        
        while True:
            query_params = {
                "limit": limit,
                "offset": offset
            }
            
            try:
                response = requests.post(
                    endpoint, 
                    headers=self.headers, 
                    params=query_params, 
                    json=payload
                )
                response.raise_for_status() 
                
                data = response.json()
                entries = data.get('entries', [])
                all_events.extend(entries)
                
                logging.info(f"Página extraída: {len(entries)} eventos (Offset: {offset}).")
                
                if len(entries) < limit:
                    break
                    
                offset += limit
                
            except requests.exceptions.RequestException as e:
                logging.error(f"Falha na comunicação com a API: {e}")
                if e.response is not None:
                    logging.error(f"Detalhes do Erro: {e.response.text}")
                raise

        logging.info(f"Sucesso! Total consolidado: {len(all_events)} eventos na costa brasileira.")
        return all_events

if __name__ == "__main__":
    extractor = GFWExtractor()
    
    hoje = datetime.now()
    semana_passada = hoje - timedelta(days=7)
    
    data_fim = hoje.strftime('%Y-%m-%d')
    data_inicio = semana_passada.strftime('%Y-%m-%d')
    
    dados = extractor.fetch_events(data_inicio, data_fim)
    
    caminho_local = f"vessel_events_brazil_{data_fim}.json"
    with open(caminho_local, "w", encoding="utf-8") as f:
        json.dump(dados, f, indent=4)
        
    logging.info(f"Arquivo salvo localmente como {caminho_local}")