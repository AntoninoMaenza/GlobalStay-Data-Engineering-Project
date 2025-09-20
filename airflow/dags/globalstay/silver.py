from airflow.decorators import task
from globalstay.azure_utils import download_csv_from_blob, upload_df_to_blob
import pandas as pd


def create_silver_tasks():
    """
    Definisce i task di pulizia e validazione (Silver).
    Ogni dataset viene trattato con regole di data quality specifiche.
    """

    @task
    def silver_hotels():
        df = download_csv_from_blob("bronze/hotels.csv")
        df = df[df["country"] != "XX"]  # Rimuove codici paese fittizi
        upload_df_to_blob(df, "silver/hotels.csv")

    @task
    def silver_customers():
        df = download_csv_from_blob("bronze/customers.csv")
        df["email"] = df["email"].replace("", None)
        df = df.drop_duplicates(subset=["customer_id"])
        upload_df_to_blob(df, "silver/customers.csv")

    @task
    def silver_rooms():
        df = download_csv_from_blob("bronze/rooms.csv")
        df = df.drop_duplicates(subset=["room_id"])
        upload_df_to_blob(df, "silver/rooms.csv")

    @task
    def silver_bookings():
        df = download_csv_from_blob("bronze/bookings.csv")

        # Converte le date e corregge eventuali inversioni
        df["checkin_date"] = pd.to_datetime(df["checkin_date"])
        df["checkout_date"] = pd.to_datetime(df["checkout_date"])
        df["original_checkin"] = df["checkin_date"]

        mask = df["original_checkin"] > df["checkout_date"]
        df.loc[mask, "checkin_date"] = df.loc[mask, "checkout_date"]
        df.loc[mask, "checkout_date"] = df.loc[mask, "original_checkin"]
        df = df.drop(columns=["original_checkin"])

        # Normalizza importi e valuta
        df["total_amount"] = df["total_amount"].apply(lambda x: None if x < 0 else x)
        df = df[df["currency"].isin(["EUR", "USD", "GBP"])]

        upload_df_to_blob(df, "silver/bookings.csv")

    @task
    def silver_payments():
        df_payments = download_csv_from_blob("bronze/payments.csv")
        df_bookings = download_csv_from_blob("bronze/bookings.csv")[["booking_id", "total_amount"]]

        df = df_payments.merge(df_bookings, on="booking_id", how="left")
        df["orphan"] = df["total_amount"].isnull()
        df["over_amount"] = df["amount"] > df["total_amount"]

        df = df.drop(columns=["total_amount"])
        df["currency"] = df["currency"].apply(lambda x: x if x in ["EUR", "USD", "GBP"] else None)

        upload_df_to_blob(df, "silver/payments.csv")

    return [
        silver_hotels(),
        silver_customers(),
        silver_rooms(),
        silver_bookings(),
        silver_payments(),
    ]
