#!/bin/bash

# 1. Get project ID and verify it
PROJECT_ID=$(gcloud config get-value project)
printf "Project ID: %s\n" "$PROJECT_ID"

# 2. Define the service account variable
SERVICE_ACCOUNT="dag-task-service-account@dag-task.iam.gserviceaccount.com"
printf "Service Account: %s\n" "$SERVICE_ACCOUNT"

# 3. Enable required APIs
echo "Enabling required APIs..."
gcloud services enable \
    composer.googleapis.com \
    compute.googleapis.com \
    container.googleapis.com \
    cloudresourcemanager.googleapis.com \
    iam.googleapis.com \
    artifactregistry.googleapis.com \
    cloudbuild.googleapis.com \
    storage-component.googleapis.com \
    monitoring.googleapis.com \
    logging.googleapis.com \
    analytics.googleapis.com \
    bigquery.googleapis.com

# 4. Add all required IAM roles to your service account for setting up composer environment
for ROLE in \
roles/composer.worker \
roles/composer.admin \
roles/composer.ServiceAgentV2Ext \
roles/storage.admin \
roles/logging.admin \
roles/monitoring.admin \
roles/container.admin \
roles/container.clusterAdmin \
roles/container.developer \
roles/compute.networkAdmin \
roles/compute.securityAdmin \
roles/bigquery.admin \
roles/storage.objectViewer \
roles/storage.objectCreator \
roles/iam.serviceAccountUser \
roles/iam.serviceAccountTokenCreator \
roles/artifactregistry.reader \
roles/datalineage.viewer \
roles/cloudbuild.builds.builder \
roles/cloudsql.client \
roles/cloudscheduler.serviceAgent \
roles/container.hostServiceAgentUser \
roles/pubsub.publisher \
roles/pubsub.subscriber \
roles/serviceusage.serviceUsageConsumer
do
    echo "Adding role: $ROLE"
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$SERVICE_ACCOUNT" \
        --role="$ROLE"
done

# 5. Add specific permissions for Google APIs Service Agent
GOOGLE_APIS_SERVICE_AGENT="$PROJECT_NUMBER@cloudservices.gserviceaccount.com"
for ROLE in \
roles/editor \
roles/iam.serviceAccountTokenCreator
do
    echo "Adding role for Google APIs Service Agent: $ROLE"
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$GOOGLE_APIS_SERVICE_AGENT" \
        --role="$ROLE"
done

# 6. Add permissions for Cloud Composer Service Agent
COMPOSER_SERVICE_AGENT="service-$PROJECT_NUMBER@cloudcomposer-accounts.iam.gserviceaccount.com"
for ROLE in \
roles/composer.serviceAgent \
roles/composer.ServiceAgentV2Ext
do
    echo "Adding role for Composer Service Agent: $ROLE"
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$COMPOSER_SERVICE_AGENT" \
        --role="$ROLE"
done

# 7. Add permissions for Container Engine Service Agent
CONTAINER_ENGINE_ROBOT="service-$PROJECT_NUMBER@container-engine-robot.iam.gserviceaccount.com"
for ROLE in \
roles/container.serviceAgent
do
    echo "Adding role for Container Engine Robot: $ROLE"
    gcloud projects add-iam-policy-binding $PROJECT_ID \
        --member="serviceAccount:$CONTAINER_ENGINE_ROBOT" \
        --role="$ROLE"
done

# 8. Verify all roles were added successfully
echo "Verifying roles for main service account:"
gcloud projects get-iam-policy $PROJECT_ID \
    --flatten="bindings[].members" \
    --format='table(bindings.role)' \
    --filter="bindings.members:$SERVICE_ACCOUNT"

# 9. Get project number (needed for service agents)
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format='value(projectNumber)')
echo "Project Number: $PROJECT_NUMBER"

echo "Setup complete! Please check for any errors above."