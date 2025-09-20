from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.operators.empty import EmptyOperator

from globalstay.bronze import create_bronze_tasks
from globalstay.silver import create_silver_tasks
from globalstay.gold import create_gold_tasks
from globalstay.ml import create_ml_tasks
from globalstay.report import create_report_tasks


@dag(
    dag_id="globalstay_etl_pipeline",
    start_date=days_ago(1),
    schedule="@daily",
    catchup=False,
    tags=["data-engineering", "etl", "globalstay"],
)
def globalstay_etl_pipeline():
    """
    Pipeline ETL principale di GlobalStay:
    1. Bronze: ingestione dati grezzi
    2. Silver: pulizia e validazione
    3. Gold: calcolo KPI
    4. ML: modello di predizione prezzi
    5. Report: generazione report finale
    """

    files = ["hotels", "rooms", "customers", "bookings", "payments"]

    # Bronze Layer
    ingest_tasks = create_bronze_tasks(files)

    # Silver Layer
    bronze_to_silver = EmptyOperator(task_id="bronze_to_silver_bridge")
    silver_tasks = create_silver_tasks()

    # Gold Layer
    silver_to_gold = EmptyOperator(task_id="silver_to_gold_bridge")
    gold_tasks = create_gold_tasks()

    # ML Layer
    ml_task = create_ml_tasks()

    # Report Layer
    report_task = create_report_tasks()

    # Definizione delle dipendenze
    ingest_tasks >> bronze_to_silver
    bronze_to_silver >> silver_tasks
    silver_tasks >> silver_to_gold

    # Gold e ML possono girare in parallelo
    silver_to_gold >> gold_tasks
    silver_to_gold >> ml_task

    # Report dipende sia dai KPI Gold sia dal modello ML
    gold_tasks >> report_task
    ml_task >> report_task


dag = globalstay_etl_pipeline()
