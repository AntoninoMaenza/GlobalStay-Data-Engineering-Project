import dlt
from pyspark.sql import functions as F

# ==============================
# Silver Layer - Pulizia + DQ
# ==============================

@dlt.table(name="globalstay.silver.hotels")
@dlt.expect_or_drop("country_valid", "country != 'XX'")
def silver_hotels():
    return dlt.read("globalstay.bronze.hotels")


@dlt.table(name="globalstay.silver.customers")
def silver_customers():
    return (
        dlt.read("globalstay.bronze.customers")
        .withColumn(
            "email",
            F.when(F.col("email") == "", F.lit(None)).otherwise(F.col("email")),
        )
        .dropDuplicates(["customer_id"])
    )


@dlt.table(name="globalstay.silver.rooms")
def silver_rooms():
    return dlt.read("globalstay.bronze.rooms").dropDuplicates(["room_id"])


@dlt.table(name="globalstay.silver.bookings")
def silver_bookings():
    return (
        dlt.read("globalstay.bronze.bookings")
        # Correzione date invertite
        .withColumn("original_checkin", F.col("checkin_date"))
        .withColumn(
            "checkin_date",
            F.when(F.col("original_checkin") > F.col("checkout_date"), F.col("checkout_date"))
            .otherwise(F.col("checkin_date")),
        )
        .withColumn(
            "checkout_date",
            F.when(F.col("original_checkin") > F.col("checkout_date"), F.col("original_checkin"))
            .otherwise(F.col("checkout_date")),
        )
        .drop("original_checkin")
        # Normalizzazione importi
        .withColumn(
            "total_amount",
            F.when(F.col("total_amount") < 0, F.lit(None)).otherwise(F.col("total_amount")),
        )
        .filter(F.col("currency").isin("EUR", "USD", "GBP"))
    )


@dlt.table(name="globalstay.silver.payments")
def silver_payments():
    bookings = dlt.read("globalstay.bronze.bookings").select("booking_id", "total_amount")

    return (
        dlt.read("globalstay.bronze.payments")
        .join(bookings, "booking_id", "left")
        .withColumn("orphan", F.col("total_amount").isNull())
        .withColumn("over_amount", F.col("amount") > F.col("total_amount"))
        .drop("total_amount")
        .withColumn(
            "currency",
            F.when(F.col("currency").isin("EUR", "USD", "GBP"), F.col("currency"))
            .otherwise(F.lit(None)),
        )
    )
