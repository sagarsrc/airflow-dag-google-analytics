from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def print_hello():
    print("Hello, World!")


def print_date():
    print(datetime.now())


default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 10, 1),
}

dag = DAG(
    "new_hello_world_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
)

hello_task = PythonOperator(
    task_id="print_hello",
    python_callable=print_hello,
    dag=dag,
)

date_task = PythonOperator(
    task_id="print_date",
    python_callable=print_date,
    dag=dag,
)

hello_task >> date_task
