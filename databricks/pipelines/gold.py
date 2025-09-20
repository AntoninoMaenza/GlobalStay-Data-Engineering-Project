import dlt
from pyspark.sql import functions as F


# ==============================
# Gold Layer - KPI Business
# ==============================

@dlt.table(name="globalstay.gold.daily_revenue")
def gold_daily_revenue():
    return (
        dlt.read("globalstay.silver.bookings")
        .filter(
            (F.col("status") == "confirmed")
            & (F.col("total_amount").isNotNull())
            & (F.col("currency").isNotNull())
        )
        .groupBy("checkin_date")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("gross_revenue"),
            F.count("*").alias("bookings_count"),
        )
        .withColumnRenamed("checkin_date", "date")
        .orderBy("date")
    )


@dlt.table(name="globalstay.gold.cancellation_rate")
def gold_cancellation_rate():
    return (
        dlt.read("globalstay.silver.bookings")
        .groupBy("source")
        .agg(
            F.count("*").alias("total_bookings"),
            F.sum(F.when(F.col("status") == "cancelled", 1).otherwise(0)).alias("cancelled"),
        )
        .withColumn("cancellation_rate_pct", (F.col("cancelled") / F.col("total_bookings")) * 100)
    )


@dlt.table(name="globalstay.gold.collection_rate")
def gold_collection_rate():
    return (
        dlt.read("globalstay.silver.payments")
        .filter(
            (F.col("orphan") == False)
            & (F.col("over_amount") == False)
            & (F.col("currency").isNotNull())
        )
        .join(dlt.read("globalstay.silver.bookings"), "booking_id", "left")
        .groupBy("hotel_id")
        .agg(
            F.sum("total_amount").alias("total_bookings_value"),
            F.sum("amount").alias("total_payments_value"),
        )
        .withColumn("collection_rate", F.col("total_payments_value") / F.col("total_bookings_value"))
    )


@dlt.table(name="globalstay.gold.overbooking_alerts")
def gold_overbooking():
    bookings = (
        dlt.read("globalstay.silver.bookings")
        .filter(F.col("status") != "cancelled")
        .select("room_id", "booking_id", "checkin_date", "checkout_date")
    )

    b1 = bookings.alias("b1")
    b2 = bookings.alias("b2")

    return (
        b1.join(
            b2,
            (F.col("b1.room_id") == F.col("b2.room_id"))
            & (F.col("b1.booking_id") < F.col("b2.booking_id")),
        )
        .filter(
            (F.col("b1.checkin_date") < F.col("b2.checkout_date"))
            & (F.col("b2.checkin_date") < F.col("b1.checkout_date"))
        )
        .select(
            F.col("b1.room_id"),
            F.col("b1.booking_id").alias("booking_id_1"),
            F.col("b2.booking_id").alias("booking_id_2"),
            F.greatest(F.col("b1.checkin_date"), F.col("b2.checkin_date")).alias("overlap_start"),
            F.least(F.col("b1.checkout_date"), F.col("b2.checkout_date")).alias("overlap_end"),
        )
    )


@dlt.table(name="globalstay.gold.customer_value")
def gold_customer_value():
    return (
        dlt.read("globalstay.silver.bookings")
        .groupBy("customer_id")
        .agg(
            F.count("*").alias("bookings_count"),
            F.sum("total_amount").alias("revenue_sum"),
            F.avg("total_amount").alias("avg_ticket"),
        )
    )
