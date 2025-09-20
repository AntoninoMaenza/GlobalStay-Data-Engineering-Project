import pandas as pd
import joblib
import numpy as np

from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score

from pyspark.sql import SparkSession

# ===========================================
# ML Notebook - Predizione Prezzi Prenotazioni
# ===========================================

# 1. Caricamento dati Silver
print("Caricamento dati da tabelle Silver...")
bookings = spark.read.table("globalstay.silver.bookings").toPandas()
rooms = spark.read.table("globalstay.silver.rooms").toPandas()

df = bookings.merge(rooms, on="room_id", how="left")

# Conversione date
df["checkin_date"] = pd.to_datetime(df["checkin_date"])
df["checkout_date"] = pd.to_datetime(df["checkout_date"])

# Feature engineering
df["stay_duration"] = (df["checkout_date"] - df["checkin_date"]).dt.days
df["month"] = df["checkin_date"].dt.month
df["season"] = ((df["month"] - 1) // 3) + 1
df["is_weekend"] = df["checkin_date"].dt.dayofweek.isin([5, 6]).astype(int)

room_types = {"single": 1, "double": 2, "suite": 3, "family": 4}
df["room_type_num"] = df["room_type_code"].map(room_types).fillna(2)

# Pulizia dati
df_clean = df[
    (df["total_amount"].notna())
    & (df["total_amount"] > 0)
    & (df["stay_duration"] > 0)
    & (df["max_occupancy"].notna())
].copy()

print(f"✅ Dati puliti: {len(df_clean)} record")

# Dataset ML
X = df_clean[["stay_duration", "max_occupancy", "room_type_num", "season", "is_weekend"]]
y = df_clean["total_amount"]

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

# Training modello
model = RandomForestRegressor(n_estimators=100, random_state=42)
model.fit(X_train, y_train)

# Valutazione
y_pred = model.predict(X_test)
rmse = np.sqrt(mean_squared_error(y_test, y_pred))
r2 = r2_score(y_test, y_pred)
print(f"📈 RMSE: €{rmse:.2f} | R²: {r2:.3f}")

# Predizioni su tutti i dati
df_clean["predicted_price"] = model.predict(X)
result = df_clean[
    [
        "booking_id",
        "total_amount",
        "predicted_price",
        "stay_duration",
        "max_occupancy",
        "room_type_code",
        "season",
    ]
].rename(columns={"total_amount": "actual_price"})

# Salvataggio modello in workspace
model_path = "/Workspace/Users/antonino.maenza@community.unipa.it/Progetto_finale/models/booking_price_predictor.pkl"
joblib.dump(model, model_path)
print(f"Modello salvato: {model_path}")

# Creazione tabella Gold
spark_gold_df = spark.createDataFrame(result)
spark_gold_df.write.mode("overwrite").format("delta").saveAsTable("globalstay.gold.predicted_prices")
print("Tabella 'globalstay.gold.predicted_prices' creata")
