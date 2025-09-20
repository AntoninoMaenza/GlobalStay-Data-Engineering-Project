from azure.storage.blob import BlobServiceClient
from airflow.hooks.base import BaseHook
import pandas as pd
from io import StringIO, BytesIO

# Recupera la connection configurata in Airflow
conn = BaseHook.get_connection("azure_blob_storage")
AZURE_CONNECTION_STRING = conn.extra_dejson.get("connection_string")
CONTAINER_NAME = "globalstay"

# Inizializza il client di Azure Blob
blob_service_client = BlobServiceClient.from_connection_string(AZURE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(CONTAINER_NAME)


# ========================
# Funzioni per file CSV
# ========================

def upload_df_to_blob(df: pd.DataFrame, blob_path: str):
    """
    Carica un DataFrame Pandas come CSV all’interno di Azure Blob Storage.
    
    Args:
        df (pd.DataFrame): DataFrame da salvare.
        blob_path (str): Path (nome blob) all’interno del container.
    """
    output = StringIO()
    df.to_csv(output, index=False)
    container_client.upload_blob(blob_path, output.getvalue(), overwrite=True)


def download_csv_from_blob(blob_path: str) -> pd.DataFrame:
    """
    Scarica un CSV da Azure Blob Storage e lo restituisce come DataFrame Pandas.
    
    Args:
        blob_path (str): Path del blob da leggere.
    """
    blob_client = container_client.get_blob_client(blob_path)
    data = blob_client.download_blob().readall().decode("utf-8")
    return pd.read_csv(StringIO(data))


# ========================
# Funzioni per file binari
# ========================

def upload_bytes_to_blob(content: bytes, blob_path: str):
    """
    Carica un file binario (es. modello .pkl) su Azure Blob Storage.
    
    Args:
        content (bytes): Contenuto binario da caricare.
        blob_path (str): Path del blob in cui salvare.
    """
    container_client.upload_blob(blob_path, content, overwrite=True)
