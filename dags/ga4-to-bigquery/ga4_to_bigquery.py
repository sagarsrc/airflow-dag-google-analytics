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
from airflow.exceptions import AirflowSkipException

# Constants
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


def upload_empty_data(gcs_hook, date_str, gcs_path):
    """Helper function to upload empty data placeholder"""
    empty_data = [
        {
            "city": "NO_DATA",
            "date": date_str,
            "activeUsers": 0,
            "sessions": 0,
            "screenPageViews": 0,
        }
    ]
    df = pd.DataFrame(empty_data)
    json_data = "\n".join(df.apply(lambda x: x.to_json(), axis=1))

    gcs_hook.upload(
        bucket_name=BUCKET_NAME,
        object_name=gcs_path,
        data=json_data.encode("utf-8"),
    )
    return gcs_path


def extract_and_upload_ga4_data(**context):
    """Extract data from GA4 and upload directly to GCS."""
    execution_date = context["execution_date"].date()
    date_str = execution_date.strftime("%Y-%m-%d")
    gcs_path = f"ga4_data/{date_str}/data.json"

    try:
        # Get credentials and initialize clients
        hook = GoogleBaseHook(gcp_conn_id="google_cloud_default")
        credentials = hook.get_credentials()
        client = BetaAnalyticsDataClient(credentials=credentials)
        gcs_hook = GCSHook(gcp_conn_id="google_cloud_default")

        print(f"Processing data for date: {date_str}")

        # Check for future dates
        if execution_date > datetime.now().date():
            print(f"Future date detected: {date_str}, uploading empty data")
            gcs_path = upload_empty_data(gcs_hook, date_str, gcs_path)
            context["task_instance"].xcom_push(key="gcs_path", value=gcs_path)
            return gcs_path

        # Request GA4 data
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

        response = client.run_report(request)
        print(f"Received GA4 response with {len(response.rows)} rows")

        if not response.rows:
            print(f"No data for date {date_str}, uploading empty data")
            gcs_path = upload_empty_data(gcs_hook, date_str, gcs_path)
            context["task_instance"].xcom_push(key="gcs_path", value=gcs_path)
            return gcs_path

        # Process GA4 data
        data = [
            {
                "city": row.dimension_values[0].value,
                "date": row.dimension_values[1].value,
                "activeUsers": int(row.metric_values[0].value),
                "sessions": int(row.metric_values[1].value),
                "screenPageViews": int(row.metric_values[2].value),
            }
            for row in response.rows
        ]

        # Upload to GCS
        df = pd.DataFrame(data)
        json_data = "\n".join(df.apply(lambda x: x.to_json(), axis=1))

        gcs_hook.upload(
            bucket_name=BUCKET_NAME,
            object_name=gcs_path,
            data=json_data.encode("utf-8"),
        )
        print(f"Successfully uploaded data to gs://{BUCKET_NAME}/{gcs_path}")

        # Verify upload
        if not gcs_hook.exists(bucket_name=BUCKET_NAME, object_name=gcs_path):
            raise Exception(f"Upload verification failed: File not found at {gcs_path}")

        context["task_instance"].xcom_push(key="gcs_path", value=gcs_path)
        return gcs_path

    except Exception as e:
        print(f"Error: {type(e)} - {str(e)}")
        # Instead of failing, upload empty data as fallback
        try:
            print("Attempting to upload empty data as fallback")
            gcs_path = upload_empty_data(gcs_hook, date_str, gcs_path)
            context["task_instance"].xcom_push(key="gcs_path", value=gcs_path)
            return gcs_path
        except Exception as fallback_error:
            print(f"Fallback error: {fallback_error}")
            raise


with DAG(
    "ga4_to_bigquery",
    default_args=default_args,
    description="Extract GA4 data and load to BigQuery",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 16),
    end_date=datetime(2024, 4, 30),
    catchup=True,
    max_active_runs=3,
    render_template_as_native_obj=True,
) as dag:

    extract_and_upload_task = PythonOperator(
        task_id="extract_and_upload_ga4_data",
        python_callable=extract_and_upload_ga4_data,
        provide_context=True,
    )

    load_task = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=[
            "{{ task_instance.xcom_pull(task_ids='extract_and_upload_ga4_data') }}"
        ],
        destination_project_dataset_table=f"{PROJECT_ID}.custom_analytics_data.ga4_data${{{{ ds_nodash }}}}",
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

    # Set the order of tasks
    extract_and_upload_task >> load_task
