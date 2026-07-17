"""
Módulo de extração para o Azure Data Lake Storage Gen2.
Gerencia o download de arquivos do Data Lake para processamento local.
"""
import os
import logging
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ADLSExtractor:
    def __init__(self):
        self.tenant_id = os.getenv("AZURE_TENANT_ID")
        self.client_id = os.getenv("AZURE_CLIENT_ID")
        self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
        self.account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        
        self.credential = ClientSecretCredential(
            tenant_id=self.tenant_id,
            client_id=self.client_id,
            client_secret=self.client_secret
        )
        
        self.account_url = f"https://{self.account_name}.dfs.core.windows.net"
        self.service_client = DataLakeServiceClient(
            account_url=self.account_url, 
            credential=self.credential
        )

    def download_file_to_local(self, container: str, remote_file_path: str, local_file_path: str):
        """
        Faz o download de um arquivo do Data Lake para um diretório local.
        """
        try:
            file_client = self.service_client.get_file_system_client(container).get_file_client(remote_file_path)
            
            with open(local_file_path, "wb") as f:
                f.write(file_client.download_file().readall())
                
            logging.info(f"Download concluído: {container}/{remote_file_path} -> {local_file_path}")
            
        except Exception as e:
            logging.error(f"Erro ao baixar arquivo do Azure Data Lake ({remote_file_path}): {e}")
            raise   