# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


'''
This Cloud function is responsible for:
- Parsing and validating new files added to Cloud Storage.
- Checking for duplications.
- Inserting files' content into BigQuery.
- Logging the ingestion status into Cloud Firestore and Stackdriver.
- Publishing a message to either an error or success topic in Cloud Pub/Sub.
'''

import json
import logging
import os
import traceback
from datetime import datetime
import base64

from google.api_core import retry
from google.cloud import bigquery
from google.cloud import firestore
from google.cloud import pubsub_v1
from google.cloud import storage
import pytz

from utility import read_config


config = read_config()
PROJECT_ID = config[r'devshell_project_id']
BQ_DATASET = config[r'dataset']
BQ_TABLE = 'dc'
ERROR_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, config[r'streaming_error_topic'])
SUCCESS_TOPIC = 'projects/%s/topics/%s' % (PROJECT_ID, config[r'streaming_success_topic'])
DB = firestore.Client()
CS = storage.Client()
PS = pubsub_v1.PublisherClient()
BQ = bigquery.Client()

def move_file(data, context):
    '''This function is executed from a Cloud Pub/Sub'''
    message = base64.b64decode(data['data']).decode('utf-8')
    logging.info("move_file receive the message {}".format(message))
    file_name = data['attributes']['file_name']

    source_bucket_name = os.getenv('SOURCE_BUCKET')
    source_bucket = CS.get_bucket(source_bucket_name.split(r'gs://')[1])
    source_blob = source_bucket.blob(file_name)

    destination_bucket_name = os.getenv('DESTINATION_BUCKET')
    destination_bucket = CS.get_bucket(destination_bucket_name.split(r'gs://')[1])


    logging.info('Start File \'%s\' move from \'%s\' to \'%s\': \'%s\'',
                 file_name,
                 source_bucket_name,
                 destination_bucket_name,
                 message)

    source_bucket.copy_blob(source_blob, destination_bucket, file_name)
    source_blob.delete()

    logging.info('Finish file \'%s\' move from \'%s\' to \'%s\': \'%s\'',
                 file_name,
                 source_bucket_name,
                 destination_bucket_name,
                 message)

def ingest_file_external(data, context):
    '''This function is executed whenever a file is added to Cloud Storage'''
    bucket_name = data['bucket']
    file_name = data['name']
    file_path = 'gs://{}/{}'.format(bucket_name, file_name)
    logging.info('Triggered by file {}'.format(file_path))

    db_ref = DB.document(u'streaming_files/%s' % file_name)

    dataset_ref = BQ.dataset(BQ_DATASET)
    schema = [
        bigquery.SchemaField('hash', 'STRING'),
        bigquery.SchemaField('size', 'STRING'),
        bigquery.SchemaField('stripped_size', 'STRING'),
        bigquery.SchemaField('weight', 'STRING'),
        bigquery.SchemaField('number', 'STRING'),
        bigquery.SchemaField('version', 'STRING'),
        bigquery.SchemaField('merkle_root', 'STRING'),
        bigquery.SchemaField('timestamp', 'STRING'),
        bigquery.SchemaField('timestamp_month', 'STRING'),
        bigquery.SchemaField('nonce', 'STRING'),
        bigquery.SchemaField('bits', 'STRING'),
        bigquery.SchemaField('coinbase_param', 'STRING'),
        bigquery.SchemaField('transaction_count', 'STRING')
    ]
    table_suffix = _table_suffix()
    table_id = '{}_{}'.format(BQ_TABLE, table_suffix)
    logging.info('Start table creation {}'.format(table_id))

    table = bigquery.Table(dataset_ref.table(table_id), schema=schema)

    external_config = bigquery.ExternalConfig('CSV')
    external_config.source_uris = [file_path]
    external_config.options.skip_leading_rows = 1

    table.external_data_configuration = external_config

    if _was_already_ingested(db_ref):
        _handle_duplication(db_ref)
    else:
        try:
            BQ.create_table(table)
            _handle_success(db_ref)
            logging.info('table creation success {}'.format(table_id))
        except Exception:
            _handle_error(db_ref)
            logging.info('table creation fails {}'.format(table_id))


def ingest_file(data, context):
    '''This function is executed whenever a file is added to Cloud Storage'''
    bucket_name = data['bucket']
    file_name = data['name']
    file_path = 'gs://{}/{}'.format(bucket_name, file_name)
    logging.info('Triggered by file {}'.format(file_path))

    db_ref = DB.document(u'streaming_files/%s' % file_name)

    dataset_ref = BQ.dataset(BQ_DATASET)
    schema = [
        bigquery.SchemaField('hash', 'STRING'),
        bigquery.SchemaField('size', 'STRING'),
        bigquery.SchemaField('stripped_size', 'STRING'),
        bigquery.SchemaField('weight', 'STRING'),
        bigquery.SchemaField('number', 'STRING'),
        bigquery.SchemaField('version', 'STRING'),
        bigquery.SchemaField('merkle_root', 'STRING'),
        bigquery.SchemaField('timestamp', 'STRING'),
        bigquery.SchemaField('timestamp_month', 'STRING'),
        bigquery.SchemaField('nonce', 'STRING'),
        bigquery.SchemaField('bits', 'STRING'),
        bigquery.SchemaField('coinbase_param', 'STRING'),
        bigquery.SchemaField('transaction_count', 'STRING')
    ]
    table_suffix = _table_suffix()
    table_id = '{}_{}'.format(BQ_TABLE, table_suffix)
    logging.info('Start table creation {}'.format(table_id))

    #table = bigquery.Table(dataset_ref.table(table_id), schema=schema)

    job_config = bigquery.LoadJobConfig()
    job_config.schema = schema
    job_config.skip_leading_rows = 1
    job_config.source_format = bigquery.SourceFormat.CSV

    if _was_already_ingested(db_ref):
        _handle_duplication(db_ref)
    else:
        try:
            BQ.load_table_from_uri(file_path, dataset_ref.table(table_id), job_config=job_config)
            _handle_success(db_ref)
            logging.info('table creation success {}'.format(table_id))
        except Exception:
            _handle_error(db_ref)
            logging.info('table creation fails {}'.format(table_id))


def _was_already_ingested(db_ref):
    status = db_ref.get()
    return status.exists and status.to_dict()['success']


def _handle_duplication(db_ref):
    dups = [_now()]
    data = db_ref.get().to_dict()
    if 'duplication_attempts' in data:
        dups.extend(data['duplication_attempts'])

    db_ref.update({
        'duplication_attempts': dups
    })

    logging.warning('Duplication attempt ingest_file file \'%s\'' % db_ref.id)


def _insert_into_bigquery(bucket_name, file_name):
    blob = CS.get_bucket(bucket_name).blob(file_name)
    row = json.loads(blob.download_as_string())
    table = BQ.dataset(BQ_DATASET).table(BQ_TABLE)
    errors = BQ.insert_rows_json(table,
                                 json_rows=[row],
                                 row_ids=[file_name],
                                 retry=retry.Retry(deadline=30))
    if errors != []:
        raise BigQueryError(errors)


def _handle_success(db_ref):
    message = 'File \'%s\' streamed into BigQuery' % db_ref.id
    doc = {
        u'success': True,
        u'when': _now()
    }
    db_ref.set(doc)
    PS.publish(SUCCESS_TOPIC, message.encode('utf-8'), file_name=db_ref.id)
    logging.info(message)


def _handle_error(db_ref):
    message = 'Error ingest_file file \'%s\'. Cause: %s' % (db_ref.id, traceback.format_exc())
    doc = {
        u'success': False,
        u'error_message': message,
        u'when': _now()
    }
    db_ref.set(doc)
    PS.publish(ERROR_TOPIC, message.encode('utf-8'), file_name=db_ref.id)
    logging.error(message)


def _now():
    return datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y-%m-%d %H:%M:%S %Z')

def _table_suffix():
    return datetime.utcnow().replace(tzinfo=pytz.utc).strftime('%Y%m%d%H%M%S')

class BigQueryError(Exception):
    '''Exception raised whenever a BigQuery error happened''' 

    def __init__(self, errors):
        super().__init__(self._format(errors))
        self.errors = errors

    def _format(self, errors):
        err = []
        for error in errors:
            err.extend(error['errors'])
        return json.dumps(err)




