#!/usr/bin/env bash

source functions/ingest_file/config.ini

# Set up Bigquery dataset
bq ls mddataset;
return_value=$?
if [ $return_value == 1 ]; then
    echo "dataset does not exist and create"
    bq mk ${DATASET}
else
     echo "dataset ready - " ${DATASET}
fi

# Set up a source data bucket and three cloud function stage buckets
gsutil ls ${FILES_SOURCE};
return_value=$?
if [ $return_value == 1 ]; then
    echo "bucket does not exist and create" +  ${FILES_SOURCE}
    gsutil mb ${FILES_SOURCE}
else
    echo "bucket ready - " ${FILES_SOURCE}
fi

gsutil ls ${FUNCTIONS_BUCKET};
return_value=$?
if [ $return_value == 1 ]; then
    echo "bucket does not exist and create"
    gsutil mb -c regional -l ${REGION} ${FUNCTIONS_BUCKET}
else
    echo "bucket ready - " ${FUNCTIONS_BUCKET}
fi

gsutil ls ${FILES_ERROR};
return_value=$?
if [ $return_value == 1 ]; then
    echo "bucket does not exist and create"
    gsutil mb ${FILES_ERROR}
else
    echo "bucket ready - " ${FILES_ERROR}
fi



gsutil ls ${FILES_SUCCESS};
return_value=$?
if [ $return_value == 1 ]; then
    echo "bucket does not exist and create"
    gsutil mb ${FILES_SUCCESS}
else
    echo "bucket ready - " ${FILES_SUCCESS}
fi

# Set up PubSub topic for error and success file sourcing
gcloud pubsub topics describe ${STREAMING_ERROR_TOPIC};
return_value=$?
if [ $return_value == 1 ]; then
    echo "topic does not exist and create"
    gcloud pubsub topics create ${STREAMING_ERROR_TOPIC}
else
    echo "topic ready - " ${STREAMING_ERROR_TOPIC}
fi


gcloud pubsub topics describe ${STREAMING_SUCCESS_TOPIC};
return_value=$?
if [ $return_value == 1 ]; then
    echo "topic does not exist and create"
    gcloud pubsub topics create ${STREAMING_SUCCESS_TOPIC}
else
    echo "topic ready - " ${STREAMING_SUCCESS_TOPIC}
fi


# Deploy cloud function - Streaming
gcloud functions deploy streaming --region=${REGION} \
    --source=./functions/ingest_file --runtime=python37 \
    --entry-point=ingest_file \
    --timeout=540\
    --stage-bucket=${FUNCTIONS_BUCKET} \
    --trigger-bucket=${FILES_SOURCE}
gcloud functions describe streaming  --region=${REGION} \
    --format="table[box](entryPoint, status, eventTrigger.eventType)"


# Deploy cloud function - move_file
gcloud functions deploy streaming_error --region=${REGION} \
    --source=./functions/ingest_file \
    --entry-point=move_file --runtime=python37 \
    --timeout=540\
    --stage-bucket=${FUNCTIONS_BUCKET} \
    --trigger-topic=${STREAMING_ERROR_TOPIC} \
    --set-env-vars SOURCE_BUCKET=${FILES_SOURCE},DESTINATION_BUCKET=${FILES_ERROR}
gcloud functions describe streaming_error --region=${REGION} \
    --format="table[box](entryPoint, status, eventTrigger.eventType)"


# Deploy cloud function - streaming_success
gcloud functions deploy streaming_success --region=${REGION} \
    --source=./functions/ingest_file \
    --entry-point=move_file --runtime=python37 \
    --stage-bucket=${FUNCTIONS_BUCKET} \
    --trigger-topic=${STREAMING_SUCCESS_TOPIC} \
    --set-env-vars SOURCE_BUCKET=${FILES_SOURCE},DESTINATION_BUCKET=${FILES_SUCCESS}
gcloud functions describe streaming_success  --region=${REGION} \
    --format="table[box](entryPoint, status, eventTrigger.eventType)"
