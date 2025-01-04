# Google Analytics to BigQuery Data Pipeline

This project implements an Airflow DAG that extracts data from Google Analytics 4 and loads it into BigQuery.

## Prerequisites

- Python and pip installed
- Google Cloud Platform account
- Access to a Google Analytics 4 property
- Git

## Setup Instructions

### 1. Local Airflow Setup

```bash
# Install Apache Airflow
pip install apache-airflow

# Set Airflow home directory
export AIRFLOW_HOME=$(pwd)/airflow

# Start Airflow in standalone mode
airflow standalone

# Copy DAGs to Airflow directory
cp -r dags/* airflow/dags/

# Reserialize DAGs
airflow dags reserialize

# To stop Airflow when needed
pkill -f airflow
```

### 2. GCP Project Setup

1. Create a new GCP project
2. Create a service account
3. Execute `gcp_commands.sh` to:
   - Set up service account permissions
   - Configure Cloud Composer environment
4. Create Cloud Composer (Airflow) environment
5. Store the service account key JSON in the `secrets` folder

### 3. Google Analytics OAuth Setup

1. Visit [Google Cloud Console Credentials](https://console.cloud.google.com/apis/credentials)
2. Create OAuth 2.0 Client ID with the following scopes:
   - `https://www.googleapis.com/auth/analytics.readonly`
   - `https://www.googleapis.com/auth/cloud-platform`
   - `https://www.googleapis.com/auth/analytics`
3. Store the OAuth 2.0 Client ID JSON in the `secrets` folder

### 4. Google Analytics Configuration

1. Link your Analytics account to the GCP project
2. Note down the property ID
3. Add the service account email to Analytics account (Property Management Settings)
4. Run the test script to verify connection

### 5. Airflow Configuration

1. Configure Google Cloud Connection:
   - Navigate to Admin > Connections > google_cloud_default
   - Add service account key JSON
   - Add Analytics scopes in Extra field:
     ```
     https://www.googleapis.com/auth/analytics.readonly,https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/analytics
     ```

## CI/CD

GitHub Actions workflow automatically pushes DAGs to the Cloud Composer environment's storage bucket.

## Verification

### Expected Setup Views

1. Cloud Composer Environment:

   Initial setup:
   ![Initial Composer Setup](docs/images/composer1.jpg)

   After DAG execution:
   ![Composer After DAG Run](docs/images/composer2.jpg)

2. Airflow Interface:

   Successful DAG run:
   ![Successful Airflow DAG](docs/images/airflow.jpg)

3. GCP Storage Buckets:

   GA4 Events bucket:
   ![GA4 Events Bucket](docs/images/gcp-ga4-bucket.jpg)

   DAGs and logs bucket:
   ![DAGs and Logs Bucket](docs/images/gcp-dags-bucket.jpg)

### Data Validation

Run `test_google_analytics.py` to verify data consistency. Output:

```bash
         date  activeUsers
0  2024-12-16            1
1  2024-12-18            2
2  2024-12-19            4
3  2024-12-21            1
4  2024-12-22            2
5  2024-12-23            1
6  2024-12-24            2
7  2024-12-26            6
8  2024-12-30            1
```

Cross-verify with BigQuery using:

```sql
SELECT
  PARSE_DATE('%Y%m%d', CAST(date AS STRING)) as date,
  SUM(activeUsers) as daily_active_users
FROM
  `dag-task.custom_analytics_data.ga4_data`
WHERE
  PARSE_DATE('%Y%m%d', CAST(date AS STRING))
    BETWEEN PARSE_DATE('%Y%m%d', '20241216')
    AND PARSE_DATE('%Y%m%d', '20241231')
GROUP BY
  date_formatted
ORDER BY
  date_formatted;
```

Output:

```csv
date,daily_active_users
2024-12-16,1
2024-12-18,2
2024-12-19,4
2024-12-21,1
2024-12-22,2
2024-12-23,1
2024-12-24,2
2024-12-26,6
2024-12-30,1
```

## Note

The Google Analytics demo account cannot be used with the Analytics Data API due to permissions limitations.
