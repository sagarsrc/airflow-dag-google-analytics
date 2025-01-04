from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from datetime import datetime, timedelta
import pandas as pd
import tempfile
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
import os
import json

# Constants from your environment
PROJECT_ID = "dag-task"
REGION = "us-central1"
BUCKET_NAME = "data-ga4-bucket"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


def create_temp_credentials(**context):
    """Create temporary credentials file and return its path."""
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".json", delete=False
        ) as cred_file:
            cred_file.write(os.environ.get("GCP_SERVICE_ACCOUNT_SECRET"))
            return cred_file.name
    except Exception as e:
        print(f"Error creating credentials file: {str(e)}")
        raise


def extract_ga4_data(**context):
    """Extract data from GA4 and save to a temporary file."""
    temp_files = []

    try:
        # Create temporary credentials file
        cred_path = create_temp_credentials()
        temp_files.append(cred_path)

        # Set credentials
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = cred_path

        # Initialize GA4 client
        client = BetaAnalyticsDataClient()

        # Get yesterday's date
        yesterday = context["execution_date"].date() - timedelta(days=1)
        yesterday_str = yesterday.strftime("%Y-%m-%d")

        # Build request
        request = RunReportRequest(
            property=f"properties/445872593",
            dimensions=[
                Dimension(name="city"),
                Dimension(name="date"),
            ],
            metrics=[
                Metric(name="activeUsers"),
                Metric(name="sessions"),
                Metric(name="screenPageViews"),
            ],
            date_ranges=[DateRange(start_date=yesterday_str, end_date=yesterday_str)],
        )

        # Run report
        response = client.run_report(request)

        # Convert to DataFrame
        data = []
        for row in response.rows:
            data.append(
                {
                    "city": row.dimension_values[0].value,
                    "date": row.dimension_values[1].value,
                    "activeUsers": int(row.metric_values[0].value),
                    "sessions": int(row.metric_values[1].value),
                    "screenPageViews": int(row.metric_values[2].value),
                }
            )

        df = pd.DataFrame(data)

        # Save to temporary file
        data_file = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        temp_files.append(data_file.name)
        df.to_json(data_file.name, orient="records", lines=True)

        # Store file paths for cleanup
        context["task_instance"].xcom_push(key="temp_files", value=temp_files)

        return data_file.name

    except Exception as e:
        print(f"Error type: {type(e)}")
        print(f"Error message: {str(e)}")
        # Cleanup in case of error
        for file_path in temp_files:
            if os.path.exists(file_path):
                os.unlink(file_path)
        raise


def cleanup_temp_files(**context):
    """Clean up all temporary files."""
    temp_files = context["task_instance"].xcom_pull(
        key="temp_files", task_ids="extract_ga4_data"
    )
    if temp_files:
        for file_path in temp_files:
            if os.path.exists(file_path):
                os.unlink(file_path)


with DAG(
    "ga4_to_bigquery",
    default_args=default_args,
    description="Extract GA4 data and load to BigQuery",
    schedule_interval="0 1 * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Task 1: Extract GA4 data
    extract_task = PythonOperator(
        task_id="extract_ga4_data",
        python_callable=extract_ga4_data,
        provide_context=True,
    )

    # Task 2: Upload to GCS
    upload_to_gcs_task = LocalFilesystemToGCSOperator(
        task_id="upload_to_gcs",
        src="{{ task_instance.xcom_pull(task_ids='extract_ga4_data') }}",
        dst="ga4_data/{{ ds }}/data.json",
        bucket=BUCKET_NAME,
        gcp_conn_id="google_cloud_default",
    )

    # Task 3: Cleanup temporary files
    cleanup_task = PythonOperator(
        task_id="cleanup_temp_files",
        python_callable=cleanup_temp_files,
        provide_context=True,
    )

    # Task 4: Load to BigQuery
    load_to_bq_task = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=["ga4_data/{{ ds }}/data.json"],
        # need to create a new table and then use it
        destination_project_dataset_table=f"{PROJECT_ID}.analytics_data.ga4_data${{{{ ds_nodash }}}}",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "date", "type": "STRING", "mode": "NULLABLE"},
            {"name": "activeUsers", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "sessions", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "screenPageViews", "type": "INTEGER", "mode": "NULLABLE"},
        ],
    )

    # Set task dependencies
    extract_task >> upload_to_gcs_task >> cleanup_task >> load_to_bq_task
