import dlt
from pyspark.sql import functions as F

# ==============================
# Bronze Layer - Ingestione dati
# ==============================

def ingest_one(name: str):
    """
    Ingestione di un singolo file CSV dal volume `landing/`
    verso la tabella Bronze corrispondente.
    """
    input_path = f"/Volumes/globalstay/raw/landing/{name}.csv"
    out_path = f"globalstay.bronze.{name}"

    @dlt.table(name=out_path)
    def bronze_table():
        return (
            spark.read.option("header", "true")
            .option("inferSchema", "true")
            .csv(input_path)
            .withColumn("ingestion_date", F.current_timestamp())
        )


# Elenco dei file da ingestire
files = ["hotels", "rooms", "customers", "bookings", "payments"]

for name in files:
    ingest_one(name)
