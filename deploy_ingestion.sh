#!/usr/bin/env bash

source functions/ingest_file/config.ini

#REGION=europe-west2
#DEVSHELL_PROJECT_ID=ab-scanner-dev
#FUNCTIONS_BUCKET=gs://${DEVSHELL_PROJECT_ID}-functions-dm
#FILES_SOURCE=gs://${DEVSHELL_PROJECT_ID}-files-source-dm

gcloud functions deploy streaming --region=${REGION} \
    --source=./functions/ingest_file --runtime=python37 \
    --entry-point=ingest_file \
    --timeout=540\
    --stage-bucket=${FUNCTIONS_BUCKET} \
    --trigger-bucket=${FILES_SOURCE}
gcloud functions describe streaming  --region=${REGION} \
    --format="table[box](entryPoint, status, eventTrigger.eventType)"