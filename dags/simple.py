from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import os

default_args = {
    "owner": "you",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def load_csv_to_db(**context):
    csv_file_path = os.path.join(os.path.dirname(__file__), "sample.csv")
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"{csv_file_path} not found on the worker")
    df = pd.read_csv(csv_file_path)
    hook = PostgresHook(postgres_conn_id="localdb")
    engine = hook.get_sqlalchemy_engine()

    try:
        df.to_sql(
            "servicenow_tickets",
            engine,
            if_exists="append",
            index=False,
            method="multi"
        )
    finally:
        try:
            engine.dispose()
        except Exception:
            pass


with DAG(
    dag_id="inject_task",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["example"],
) as dag:

    start = EmptyOperator(task_id="start")

    inject_task = PythonOperator(
        task_id="load_csv",
        python_callable=load_csv_to_db,
        provide_context=True,
    )

    run_dbt_models = BashOperator(
        task_id="run_dbt_models",
        bash_command="cd /Users/abishekmani/desktop/digitalxc_assignment && dbt run",
        env={"PATH": "/usr/local/bin:/usr/bin:$PATH"},
    )

    end = EmptyOperator(task_id="end")

    start >> inject_task >> run_dbt_models >> end
