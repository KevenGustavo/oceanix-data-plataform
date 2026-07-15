"""
Módulo de carga para o Azure Data Lake Storage Gen2.
Gerencia a autenticação via Service Principal e o upload de arquivos para a camada Bronze.
"""
import os
import logging
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ADLSLoader:
    def __init__(self):
        self.tenant_id = os.getenv("AZURE_TENANT_ID")
        self.client_id = os.getenv("AZURE_CLIENT_ID")
        self.client_secret = os.getenv("AZURE_CLIENT_SECRET")
        self.account_name = os.getenv("AZURE_STORAGE_ACCOUNT_NAME")
        
        # Autenticação segura usando o Service Principal
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

    def upload_json_to_bronze(self, json_string: str, path: str, file_name: str):
        """
        Faz o upload de uma string JSON para o container 'bronze'.
        Cria a estrutura de diretórios dinamicamente (particionamento).
        """
        try:
            # 1. Conecta ao container bronze
            file_system_client = self.service_client.get_file_system_client(file_system="bronze")
            
            # 2. Conecta/Cria o diretório particionado
            directory_client = file_system_client.get_directory_client(path)
            directory_client.create_directory()
            
            # 3. Cria o arquivo e faz o upload dos dados brutos
            file_client = directory_client.get_file_client(file_name)
            file_client.upload_data(json_string, overwrite=True)
            
            logging.info(f"Upload concluído com sucesso: bronze/{path}/{file_name}")
            
        except Exception as e:
            logging.error(f"Erro ao fazer upload para o Azure Data Lake: {e}")
            raise