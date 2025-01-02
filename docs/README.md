# Task

**Data Pipelines**
Create the following dag that takes the data from `google-analytics-events` and dumps it to `bigquery`
Go through the following README for the implementation steps and demo examples
https://github.com/yral-dapp/data-science-directed-acyclic-graphs/blob/main/README.md

# setup GCP

- follow [setup.md](./setup.md) file to setup GCP and Demo analytics account

# local development airflow DAG

```bash
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

# Setting up CICD

get bucket associated with the cloud composer environment

```bash
gcloud composer environments describe dag-airflow --location us-central1
```

us-central1-dag-airflow-840b4971-bucket
us-central1-dag-airflow-840b4971-bucket
