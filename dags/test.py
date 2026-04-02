from datetime import datetime
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.standard.operators.bash import BashOperator


def extract():
    print(" Extracting data...")


def transform():
    print(" Transforming data...")


def load():
    print(" Loading data...")


with DAG(
    dag_id="etl_airflow_310_standard",
    start_date=datetime(2026, 4, 1),
    schedule="*/2 * * * *",
    catchup=False,
    tags=["airflow310", "standard_provider"],
) as dag:

    start = BashOperator(
        task_id="start",
        bash_command="echo 'Start ETL'"
    )

    t1 = PythonOperator(
        task_id="extract",
        python_callable=extract
    )

    t2 = PythonOperator(
        task_id="transform",
        python_callable=transform
    )

    t3 = PythonOperator(
        task_id="load",
        python_callable=load
    )

    end = BashOperator(
        task_id="end",
        bash_command="echo ' Done'"
    )

    start >> t1 >> t2 >> t3 >> end