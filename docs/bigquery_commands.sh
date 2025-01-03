# 1. Get project ID and verify it
PROJECT_ID=$(gcloud config get-value project)
echo "Project ID: $PROJECT_ID"

# 2. Create a dataset in bigquery
bq mk --dataset \
 --description "Google Analytics Events Data" \
 $PROJECT_ID:ga_events_dataset
