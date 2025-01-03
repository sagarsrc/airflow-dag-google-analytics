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

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "ga4_to_bigquery",
    default_args=default_args,
    description="Extract GA4 data and load to BigQuery",
    schedule_interval="0 1 * * *",  # Run at 1 AM daily
    start_date=datetime(2024, 12, 31),
    catchup=False,
)


def extract_ga4_data(**context):
    """Extract data from GA4 and save to a temporary file."""
    # Get yesterday's date
    yesterday = context["execution_date"].date() - timedelta(days=1)
    yesterday_str = yesterday.strftime("%Y-%m-%d")

    # Initialize GA4 client
    client = BetaAnalyticsDataClient()

    # Build request
    request = RunReportRequest(
        property=f"properties/445872593",  # Replace with your GA4 property ID
        dimensions=[
            Dimension(name="city"),
            Dimension(name="date"),  # Adding date dimension
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
    with tempfile.NamedTemporaryFile(delete=False, suffix=".json") as temp_file:
        df.to_json(temp_file.name, orient="records", lines=True)
        return temp_file.name


# Task 1: Extract GA4 data
extract_task = PythonOperator(
    task_id="extract_ga4_data",
    python_callable=extract_ga4_data,
    provide_context=True,
    dag=dag,
)

# Task 2: Upload to GCS
upload_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id="upload_to_gcs",
    src="{{ task_instance.xcom_pull(task_ids='extract_ga4_data') }}",
    dst="ga4_data/{{ ds }}/data.json",  # Uses execution date in path
    bucket="data-ga4-bucket",  # Replace with your bucket name
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

# Task 3: Load to BigQuery
load_to_bq_task = GCSToBigQueryOperator(
    task_id="load_to_bigquery",
    bucket=" data-ga4-bucket",
    source_objects=["ga4_data/{{ ds }}/data.json"],
    destination_project_dataset_table="dag-task.data-ga4-bucket.ga4_data_${{ ds_nodash }}",  # Partitioned table
    source_format="NEWLINE_DELIMITED_JSON",
    write_disposition="WRITE_TRUNCATE",
    schema_fields=[
        {"name": "city", "type": "STRING", "mode": "NULLABLE"},
        {"name": "date", "type": "STRING", "mode": "NULLABLE"},
        {"name": "activeUsers", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "sessions", "type": "INTEGER", "mode": "NULLABLE"},
        {"name": "screenPageViews", "type": "INTEGER", "mode": "NULLABLE"},
    ],
    dag=dag,
)

# Set task dependencies
extract_task >> upload_to_gcs_task >> load_to_bq_task
