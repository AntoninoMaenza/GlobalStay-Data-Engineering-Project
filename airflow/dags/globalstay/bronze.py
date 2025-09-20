from airflow.decorators import task
from globalstay.azure_utils import download_csv_from_blob, upload_df_to_blob
import pandas as pd


def create_bronze_tasks(files):
    """
    Definisce i task di ingestion (Bronze).
    Ogni CSV grezzo viene letto da 'landing/' e salvato in 'bronze/'
    con l’aggiunta di ingestion_date.
    """
    
    @task
    def ingest_one(name: str):
        input_blob = f"landing/{name}.csv"
        output_blob = f"bronze/{name}.csv"

        df = download_csv_from_blob(input_blob)
        df["ingestion_date"] = pd.Timestamp.now()

        upload_df_to_blob(df, output_blob)
        print(f"Bronze ingest completata per {name}")

    return [
        ingest_one.override(task_id=f"bronze_ingest_{name}")(name)
        for name in files
        ]
