"""
Orquestrador de Ingestão Diária (API -> Camada Bronze).
Idempotente: Aceita uma data específica (target_date) para extração e particionamento.
"""
import json
import logging
import argparse
from datetime import datetime, timedelta

from extractors.gfw_api import GFWExtractor
from loaders.adls_loader import ADLSLoader

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def run_ingestion(target_date_str: str):
    logging.info(f"Iniciando pipeline de ingestão para a data-alvo: {target_date_str}")
    
    extractor = GFWExtractor()
    loader = ADLSLoader()
    
    # Valida e converte a string de data
    target_date = datetime.strptime(target_date_str, '%Y-%m-%d')
    
    # A API do GFW usa startDate (inclusivo) e endDate (exclusivo).
    start_date = target_date.strftime('%Y-%m-%d')
    end_date = (target_date + timedelta(days=1)).strftime('%Y-%m-%d')
    
    dados_brutos = extractor.fetch_events(start_date, end_date)
    
    if not dados_brutos:
        logging.warning(f"Nenhum dado encontrado para {target_date_str}. Abortando.")
        return

    json_string = json.dumps(dados_brutos, ensure_ascii=False)
    
    # Particionamento estrito baseado na data-alvo
    ano = target_date.strftime('%Y')
    mes = target_date.strftime('%m')
    dia = target_date.strftime('%d')
    
    caminho_particao = f"gfw/events/year={ano}/month={mes}/day={dia}"
    nome_arquivo = "vessel_events_brazil.json"
    
    loader.upload_json_to_bronze(json_string, caminho_particao, nome_arquivo)
    logging.info(f"Ingestão diária concluída: {len(dados_brutos)} eventos salvos.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingestão de Dados do GFW para a Camada Bronze")
    parser.add_argument(
        "--date", 
        type=str, 
        help="Data alvo da extração no formato YYYY-MM-DD. Ex: 2026-07-14",
        required=True
    )
    
    args = parser.parse_args()
    run_ingestion(args.date)