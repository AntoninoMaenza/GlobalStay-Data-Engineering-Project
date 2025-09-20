from airflow.decorators import task
from globalstay.azure_utils import download_csv_from_blob, upload_df_to_blob
import pandas as pd


def create_gold_tasks():
    """
    Definisce i task per la generazione dei KPI (Gold).
    Ogni funzione produce un dataset sintetico di supporto al business.
    """

    @task
    def gold_daily_revenue():
        df = download_csv_from_blob("silver/bookings.csv")
        df = df[
            (df["status"] == "confirmed")
            & (df["total_amount"].notnull())
            & (df["currency"].notnull())
        ]

        df_agg = (
            df.groupby("checkin_date")
            .agg(
                gross_revenue=("total_amount", lambda x: round(x.sum(), 2)),
                bookings_count=("total_amount", "count"),
            )
            .reset_index()
        )

        df_agg = df_agg.rename(columns={"checkin_date": "date"}).sort_values("date")
        upload_df_to_blob(df_agg, "gold/daily_revenue.csv")

    @task
    def gold_cancellation_rate():
        df = download_csv_from_blob("silver/bookings.csv")
        df_agg = (
            df.groupby("source")
            .agg(
                total_bookings=("booking_id", "count"),
                cancelled=("status", lambda x: (x == "cancelled").sum()),
            )
            .reset_index()
        )

        df_agg["cancellation_rate_pct"] = (
            df_agg["cancelled"] / df_agg["total_bookings"]
        ) * 100

        upload_df_to_blob(df_agg, "gold/cancellation_rate.csv")

    @task
    def gold_collection_rate():
        df_payments = download_csv_from_blob("silver/payments.csv")
        df_bookings = download_csv_from_blob("silver/bookings.csv")[
            ["booking_id", "hotel_id", "total_amount"]
        ]

        df = df_payments.merge(df_bookings, on="booking_id", how="left")
        df = df[
            (df["orphan"] == False)
            & (df["over_amount"] == False)
            & (df["currency"].notnull())
        ]

        df_agg = (
            df.groupby("hotel_id")
            .agg(
                total_bookings_value=("total_amount", "sum"),
                total_payments_value=("amount", "sum"),
            )
            .reset_index()
        )

        df_agg["collection_rate"] = (
            df_agg["total_payments_value"] / df_agg["total_bookings_value"]
        )

        upload_df_to_blob(df_agg, "gold/collection_rate.csv")

    @task
    def gold_overbooking_alerts():
        df = download_csv_from_blob("silver/bookings.csv")
        df = df[df["status"] != "cancelled"]
        df = df[["room_id", "booking_id", "checkin_date", "checkout_date"]].copy()

        df["checkin_date"] = pd.to_datetime(df["checkin_date"])
        df["checkout_date"] = pd.to_datetime(df["checkout_date"])
        df = df.sort_values(["room_id", "checkin_date"]).reset_index(drop=True)

        overlaps = []
        for room_id, group in df.groupby("room_id"):
            group = group.reset_index(drop=True)
            for i in range(len(group)):
                current = group.loc[i]
                for j in range(i + 1, len(group)):
                    next_booking = group.loc[j]
                    if next_booking["checkin_date"] >= current["checkout_date"]:
                        break
                    overlaps.append(
                        {
                            "room_id": room_id,
                            "booking_id_1": current["booking_id"],
                            "booking_id_2": next_booking["booking_id"],
                            "overlap_start": max(
                                current["checkin_date"], next_booking["checkin_date"]
                            ),
                            "overlap_end": min(
                                current["checkout_date"], next_booking["checkout_date"]
                            ),
                        }
                    )

        result_df = pd.DataFrame(overlaps)
        upload_df_to_blob(result_df, "gold/overbooking_alerts.csv")

    @task
    def gold_customer_value():
        df = download_csv_from_blob("silver/bookings.csv")
        df_agg = (
            df.groupby("customer_id")
            .agg(
                bookings_count=("booking_id", "count"),
                revenue_sum=("total_amount", "sum"),
                avg_ticket=("total_amount", "mean"),
            )
            .reset_index()
        )

        upload_df_to_blob(df_agg, "gold/customer_value.csv")

    return [
        gold_daily_revenue(),
        gold_cancellation_rate(),
        gold_collection_rate(),
        gold_overbooking_alerts(),
        gold_customer_value(),
    ]
