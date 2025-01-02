from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging

# Set up logging
logger = logging.getLogger(__name__)

# Define default arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email": ["your-email@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "hello_world",
    default_args=default_args,
    description="A simple hello world DAG",
    schedule_interval=timedelta(days=1),
    catchup=False,
)


# Define the tasks with enhanced logging
def print_hello(**context):
    task_instance = context["task_instance"]
    execution_date = context["execution_date"]

    message = f"Hello World! Task executed at {datetime.now()}"
    logger.info("=" * 60)
    logger.info(message)
    logger.info(f"Execution date: {execution_date}")
    logger.info(f"Task ID: {task_instance.task_id}")
    logger.info("=" * 60)

    # Store the message in XCom for the next task
    task_instance.xcom_push(key="greeting_time", value=str(datetime.now()))

    return message


def print_date(**context):
    task_instance = context["task_instance"]

    # Get the greeting time from previous task
    greeting_time = task_instance.xcom_pull(task_ids="print_hello", key="greeting_time")

    message = f"Current date and time is: {datetime.now()}"
    logger.info("=" * 60)
    logger.info(message)
    logger.info(f"Previous task (print_hello) was executed at: {greeting_time}")
    logger.info(f"Task ID: {task_instance.task_id}")
    logger.info("=" * 60)

    return message


# Create the tasks with provide_context=True to get access to the context variables
task1 = PythonOperator(
    task_id="print_hello",
    python_callable=print_hello,
    provide_context=True,
    dag=dag,
)

task2 = PythonOperator(
    task_id="print_date",
    python_callable=print_date,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
task1 >> task2
