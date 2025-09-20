from airflow.decorators import task
import seaborn as sns
import matplotlib.pyplot as plt
import pandas as pd
import io, base64

from globalstay.azure_utils import download_csv_from_blob, container_client


@task
def generate_report():
    """
    Genera un report HTML con KPI e grafici a partire dai dataset Gold.
    """
    sns.set_theme(style="whitegrid")

    # Helper: converte un grafico Matplotlib in <img> HTML
    def plot_to_html(fig):
        buf = io.BytesIO()
        fig.savefig(buf, format="png", bbox_inches="tight")
        buf.seek(0)
        img_base64 = base64.b64encode(buf.read()).decode("utf-8")
        plt.close(fig)
        return f'<img src="data:image/png;base64,{img_base64}"/>'

    html_parts = [
        """
        <html>
        <head>
        <style>
            body { font-family: 'Segoe UI', Arial, sans-serif; margin: 40px; background:#f9f9f9; color:#2c3e50; }
            h1 { color:#2c3e50; text-align:center; }
            h2 { color:#34495e; margin-top:40px; text-align:center; }
            .chart { margin:30px auto; text-align:center; }
            .toc { margin-bottom:40px; }
            a { text-decoration:none; color:#007BFF; }
            table { border-collapse: collapse; margin: 20px auto; font-size: 14px; }
            th, td { border: 1px solid #ddd; padding: 8px 12px; text-align: center; }
            th { background:#2c3e50; color:white; }
            tr:nth-child(even) { background:#f2f2f2; }
            tr:hover { background:#ddd; }
        </style>
        </head>
        <body>
        <h1>GlobalStay - Report KPI Gold</h1>
        <p style="text-align:center;">Business KPIs and Price Prediction</p>
        <div class="toc">
            <h3>Indice</h3>
            <ul>
                <li><a href="#customer">Customer Value</a></li>
                <li><a href="#overbooking">Overbooking Alerts</a></li>
                <li><a href="#collection">Collection Rate</a></li>
                <li><a href="#cancellation">Cancellation Rate</a></li>
                <li><a href="#revenue">Daily Revenue</a></li>
                <li><a href="#predicted">Predicted vs Actual Price</a></li>
            </ul>
        </div>
        """
    ]

    # 1. Customer Value
    df = download_csv_from_blob("gold/customer_value.csv")
    top_customers = df.nlargest(10, "revenue_sum")
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(data=top_customers, y="customer_id", x="revenue_sum", palette="Blues_d", ax=ax)
    ax.set_title("Top 10 Customer Value (€)")
    html_parts.append('<div id="customer" class="chart">' + plot_to_html(fig) + "</div>")

    # 2. Overbooking Alerts
    df = download_csv_from_blob("gold/overbooking_alerts.csv")
    overbook_counts = df["room_id"].value_counts().head(10).reset_index()
    overbook_counts.columns = ["room_id", "count"]
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(data=overbook_counts, y="room_id", x="count", palette="Reds_d", ax=ax)
    ax.set_title("Top 10 Overbooked Rooms")
    html_parts.append('<div id="overbooking" class="chart">' + plot_to_html(fig) + "</div>")

    # 3. Collection Rate (tabella HTML)
    df = download_csv_from_blob("gold/collection_rate.csv")
    df_sorted = df.sort_values("collection_rate", ascending=False)
    table_html = "<h2 id='collection'>Collection Rate by Hotel</h2><table>"
    table_html += "<tr><th>Hotel</th><th>Total Bookings (€)</th><th>Total Payments (€)</th><th>Collection Rate</th></tr>"
    for _, row in df_sorted.iterrows():
        color = "red" if row["collection_rate"] < 0.95 else "green"
        table_html += f"<tr><td>{row['hotel_id']}</td><td>{row['total_bookings_value']:.2f}</td><td>{row['total_payments_value']:.2f}</td><td style='color:{color}; font-weight:bold;'>{row['collection_rate']:.2%}</td></tr>"
    table_html += "</table>"
    html_parts.append('<div class="chart">' + table_html + "</div>")

    # 4. Cancellation Rate
    df = download_csv_from_blob("gold/cancellation_rate.csv")
    fig, ax = plt.subplots(figsize=(8, 5))
    sns.barplot(data=df, x="source", y="cancellation_rate_pct", palette="Greys", ax=ax)
    ax.set_title("Cancellation Rate by Source (%)")
    html_parts.append('<div id="cancellation" class="chart">' + plot_to_html(fig) + "</div>")

    # 5. Daily Revenue
    df = download_csv_from_blob("gold/daily_revenue.csv")
    df["date"] = pd.to_datetime(df["date"])
    fig, ax = plt.subplots(figsize=(12, 5))
    sns.lineplot(data=df, x="date", y="gross_revenue", marker="o", ax=ax, color="#2c3e50")
    ax.set_title("Daily Revenue Trend (€)")
    step = max(1, len(df) // 10)
    for label in ax.get_xticklabels():
        label.set_visible(False)
    for label in ax.get_xticklabels()[::step]:
        label.set_visible(True)
    plt.xticks(rotation=45, ha="right")
    html_parts.append('<div id="revenue" class="chart">' + plot_to_html(fig) + "</div>")

    # 6. Predicted vs Actual Price
    df = download_csv_from_blob("gold/predicted_price.csv")
    fig, ax = plt.subplots(figsize=(7, 7))
    sns.scatterplot(
        data=df.sample(min(200, len(df))),
        x="actual_price",
        y="predicted_price",
        alpha=0.6,
        color="#34495e",
        ax=ax,
    )
    ax.plot(
        [df["actual_price"].min(), df["actual_price"].max()],
        [df["actual_price"].min(), df["actual_price"].max()],
        color="red",
        linestyle="--",
        label="Perfect Prediction",
    )
    ax.legend()
    ax.set_title("Predicted vs Actual Price")
    html_parts.append('<div id="predicted" class="chart">' + plot_to_html(fig) + "</div>")

    # Chiusura
    html_parts.append("</body></html>")
    final_html = "".join(html_parts)

    container_client.upload_blob("reports/final_report.html", final_html, overwrite=True)
    print("Report HTML salvato in Azure Blob")


def create_report_tasks():
    """Wrapper per istanziare il task di reportistica nel DAG."""
    return generate_report()
