# Steps to setup local airflow

```bash
# install airflow
pip install apache-airflow

# set airflow home
export AIRFLOW_HOME=$(pwd)/airflow

# airflow
airflow standalone

# copy to airflow/dags
cp -r dags/* airflow/dags/

# refresh airflow
airflow dags reserialize

# stop airflow
pkill -f airflow
```

Upload the DAG to the cloud storage bucket that is being used by the cloud composer environment manually and test

# Steps to setup GCP for the task

1. Create a new project
2. Create a service account
3. Run `gcp_commands.sh` to setup the permissions for the service account and then setup cloud composer environment
4. Create Cloud Composer (airflow) environment
5. Store secret key json in the secrets folder

# get oauth2 client credentials

This is required to get the data from external GA account

1. Go to [Google Cloud Console](https://console.cloud.google.com/apis/credentials)
2. add scopes `https://www.googleapis.com/auth/analytics.readonly,https://www.googleapis.com/auth/cloud-platform,https://www.googleapis.com/auth/analytics`
3. Setup and store OAuth 2.0 Client IDs json in the secrets folder

# Steps to get Analytics data from external GA account

1. Link your analytics account to the project
2. Get the property id
3. add service account email to the analytics account in property management settings
4. use dummy script to test the connection

---

Steps to get Analytics data (demo account - no longer needed)

1. Get GA demo account by setting up new demo account using links here [GA demo account](https://support.google.com/analytics/answer/6367342?hl=en#zippy=%2Cin-this-article)
2. For this project i am using this [Google Merchandise Store's web data](https://analytics.google.com/analytics/index/demoaccount?appstate=/p213025502)

You cannot use the demo account with the Analytics Data API for either property type. Attempts to do so result in a permissions error.

reference: https://support.google.com/analytics/answer/6367342#limitations&zippy=%2Cin-this-article
