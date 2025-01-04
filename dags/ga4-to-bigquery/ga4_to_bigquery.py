from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
import os
from io import StringIO
from airflow.providers.google.common.hooks.base_google import GoogleBaseHook

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


def extract_and_upload_ga4_data(**context):
    """Extract data from GA4 and upload directly to GCS."""
    try:
        # Get credentials from Airflow connection
        hook = GoogleBaseHook(gcp_conn_id="google_cloud_default")
        credentials = hook.get_credentials()

        # Initialize GA4 client with credentials
        client = BetaAnalyticsDataClient(credentials=credentials)

        # Get the execution date from context
        execution_date = context["execution_date"].date()
        date_str = execution_date.strftime("%Y-%m-%d")
        print(f"Querying GA4 data for date: {date_str}")

        # Verify date is within valid range
        current_date = datetime.now().date()
        if execution_date > current_date:
            print(f"Warning: Requested date {date_str} is in the future")
            return None  # Return None for future dates

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
            date_ranges=[DateRange(start_date=date_str, end_date=date_str)],
        )

        # Run report
        response = client.run_report(request)

        # Debug logging for GA4 response
        print(f"GA4 Response for date {date_str}:")
        print(f"Row count: {len(response.rows)}")

        # Handle empty response case
        if not response.rows:
            print(f"No data returned from GA4 for this {date_str}")
            return None

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
        print(f"Number of rows in DataFrame: {len(df)}")

        # Convert DataFrame to properly formatted JSONL
        json_lines = []
        for _, row in df.iterrows():
            json_lines.append(row.to_json())
        json_data = "\n".join(json_lines)

        # Generate GCS path
        gcs_path = f"ga4_data/{date_str}/data.json"

        # Upload to GCS using GCSHook
        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

        try:
            # Upload using data parameter
            gcs_hook.upload(
                bucket_name=BUCKET_NAME,
                object_name=gcs_path,
                data=json_data.encode("utf-8"),
            )
            print(f"Successfully uploaded data to gs://{BUCKET_NAME}/{gcs_path}")

            # Explicitly push to XCom
            context["task_instance"].xcom_push(key="gcs_path", value=gcs_path)
            return gcs_path

        except Exception as upload_error:
            print(f"Failed to upload to GCS: {str(upload_error)}")
            raise

    except Exception as e:
        print(f"Error in extract_and_upload_ga4_data: {type(e)} - {str(e)}")
        raise


def create_load_task(dag, write_disposition="WRITE_TRUNCATE"):
    """Create a BigQuery load task with specified write disposition."""
    return GCSToBigQueryOperator(
        task_id=f"load_to_bigquery_{write_disposition.lower()}",
        bucket=BUCKET_NAME,
        source_objects=[
            "{{ task_instance.xcom_pull(task_ids='extract_and_upload_ga4_data', key='gcs_path') }}"
        ],
        destination_project_dataset_table=f"{PROJECT_ID}.custom_analytics_data.ga4_data${{{{ ds_nodash }}}}",
        source_format="NEWLINE_DELIMITED_JSON",
        write_disposition=write_disposition,
        schema_fields=[
            {"name": "city", "type": "STRING", "mode": "NULLABLE"},
            {"name": "date", "type": "STRING", "mode": "NULLABLE"},
            {"name": "activeUsers", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "sessions", "type": "INTEGER", "mode": "NULLABLE"},
            {"name": "screenPageViews", "type": "INTEGER", "mode": "NULLABLE"},
        ],
        dag=dag,
    )


with DAG(
    "ga4_to_bigquery",
    default_args=default_args,
    description="Extract GA4 data and load to BigQuery",
    schedule_interval="@daily",
    start_date=datetime(2024, 12, 16),  # Fixed date format
    end_date=datetime(2024, 12, 30),  # Fixed date format
    catchup=True,
    max_active_runs=3,
    render_template_as_native_obj=True,
) as dag:

    # Task 1: Extract GA4 data and upload to GCS
    extract_and_upload_task = PythonOperator(
        task_id="extract_and_upload_ga4_data",
        python_callable=extract_and_upload_ga4_data,
        provide_context=True,
    )

    # Task 2: Load to BigQuery (Only using TRUNCATE mode)
    load_task = create_load_task(dag, "WRITE_TRUNCATE")

    # Set task dependencies
    extract_and_upload_task >> load_task
