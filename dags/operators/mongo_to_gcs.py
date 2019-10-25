# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
"""
MongoDB to GCS operator.
"""

import abc
import json
import datetime
import time
import unicodecsv as csv
from decimal import Decimal

from airflow.contrib.hooks.mongo_hook import MongoHook
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from tempfile import NamedTemporaryFile

class MongoToGoogleCloudStorageOperator(BaseOperator, metaclass=abc.ABCMeta):
    """
    Copy data from Postgres to Google Cloud Storage in JSON or CSV format.

    :param postgres_conn_id: Reference to a specific Postgres hook.
    :type postgres_conn_id: str
    """
    ui_color = '#a0e08c'

    @apply_defaults
    def __init__(self,
                 collection,
                 find,
                 projection,
                 bucket,
                 filename,
                 bq_field_type_map,
                 approx_max_file_size_bytes=1900000000,
                 export_format='json',
                 field_delimiter=',',
                 gzip=False,
                 schema=None,
                 parameters=None,
                 gcp_conn_id='google_cloud_default',
                 mongo_conn_id='mongo_default',
                 delegate_to=None,
                 *args,
                 **kwargs):
        super().__init__(*args, **kwargs)

        self.mongo_conn_id = mongo_conn_id
        self.collection = collection
        self.find = find
        self.projection = sorted(projection) # important for CSV!
        self.bucket = bucket
        self.filename = filename.split('.')[0] + '-{}.' + export_format
        self.schema_filename = filename.split('.')[0] + '-schema.json'
        self.approx_max_file_size_bytes = approx_max_file_size_bytes
        self.export_format = export_format.lower()
        self.field_delimiter = field_delimiter
        self.gzip = gzip
        self.schema = schema
        self.parameters = parameters
        self.gcp_conn_id = gcp_conn_id
        self.delegate_to = delegate_to
        self.parameters = parameters

        # If a field wasn't explictly mapped to a BQ type, make it a string
        self.bq_field_type_map = bq_field_type_map
        for f in projection:
            if not f in self.bq_field_type_map:
                self.bq_field_type_map[f] = 'STRING'

    def execute(self, context):

        # Query mongo
        hook = MongoHook(conn_id=self.mongo_conn_id)
        cursor = hook.find(self.collection, self.find, projection=self.projection)
        files_to_upload = self._write_local_data_files(cursor)
        schema_to_upload = self._write_local_schema_file()

        files = [f['file_name'] for f in files_to_upload]
        files_to_upload.append(schema_to_upload)

        # Flush all files before uploading
        for tmp_file in files_to_upload:
            tmp_file['file_handle'].flush()
        
        self._upload_to_gcs(files_to_upload)

        # Close all temp file handles.
        for tmp_file in files_to_upload:
            tmp_file['file_handle'].close()

        return {
            'schema': schema_to_upload['file_name'],
            'files': files
        }

    def _field_to_bigquery_schema_field(self, field, field_type):
        return {
            'name': field,
            'type': field_type,
            'mode': 'NULLABLE' # add logic here for repeated
        }

    def _write_local_schema_file(self):
        schema = [self._field_to_bigquery_schema_field(f, t) for f, t in self.bq_field_type_map.items()]
        schema = sorted(schema, key = lambda f: f['name'])
        self.log.info('Using schema for %s: %s', self.schema_filename, schema)
        tmp_schema_file_handle = NamedTemporaryFile(delete=True)
        tmp_schema_file_handle.write(json.dumps(schema, sort_keys=True).encode('utf-8'))
        schema_file_to_upload = {
            'file_name': self.schema_filename,
            'file_handle': tmp_schema_file_handle,
            'file_mime_type': 'application/json',
        }
        return schema_file_to_upload

    def _convert_bigquery_type(self, value):
        if isinstance(value, datetime.datetime):
            return value.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        if isinstance(value, Decimal):
            return float(value)
        return value

    def _write_local_data_files(self, cursor):
        """
        Takes a cursor, and writes results to a local file.

        :return: A dictionary where keys are filenames to be used as object
            names in GCS, and values are file handles to local files that
            contain the data for the GCS objects.
        """
        schema = self.projection
        file_no = 0
        tmp_file_handle = NamedTemporaryFile(delete=True)
        if self.export_format == 'csv':
            file_mime_type = 'text/csv'
        else:
            file_mime_type = 'application/json'
        files_to_upload = [{
            'file_name': self.filename.format(file_no),
            'file_handle': tmp_file_handle,
            'file_mime_type': file_mime_type
        }]

        if self.export_format == 'csv':
            csv_writer = self._configure_csv_file(tmp_file_handle, schema)

        print('cursor count:', cursor.count())
        for doc in cursor:
            # Convert datetime objects to utc seconds, and decimals to floats.
            # Convert binary type object to string encoded with base64.
            # THIS IS A TERRIBLE HACK
            row = [self._convert_bigquery_type(doc[k]) for k in schema]

            if self.export_format == 'csv':
                csv_writer.writerow(row)
            else:
                row_dict = dict(zip(schema, row))

                # TODO validate that row isn't > 2MB. BQ enforces a hard row size of 2MB.
                tmp_file_handle.write(json.dumps(row_dict, sort_keys=True).encode('utf-8'))

                # Append newline to make dumps BigQuery compatible.
                tmp_file_handle.write(b'\n')

            # Stop if the file exceeds the file size limit.
            if tmp_file_handle.tell() >= self.approx_max_file_size_bytes:
                file_no += 1
                tmp_file_handle = NamedTemporaryFile(delete=True)
                files_to_upload.append({
                    'file_name': self.filename.format(file_no),
                    'file_handle': tmp_file_handle,
                    'file_mime_type': file_mime_type
                })

                if self.export_format == 'csv':
                    csv_writer = self._configure_csv_file(tmp_file_handle, schema)

        return files_to_upload

    def _configure_csv_file(self, file_handle, schema):
        """Configure a csv writer with the file_handle and write schema
        as headers for the new file.
        """
        csv_writer = csv.writer(file_handle, encoding='utf-8',
                                delimiter=self.field_delimiter)
        csv_writer.writerow(schema)
        return csv_writer

    def _upload_to_gcs(self, files_to_upload):
        """
        Upload all of the file splits (and optionally the schema .json file) to
        Google cloud storage.
        """
        hook = GoogleCloudStorageHook(
            google_cloud_storage_conn_id=self.gcp_conn_id,
            delegate_to=self.delegate_to)
        for tmp_file in files_to_upload:
            hook.upload(self.bucket, tmp_file.get('file_name'),
                        tmp_file.get('file_handle').name,
                        mime_type=tmp_file.get('file_mime_type'),
                        gzip=self.gzip if tmp_file.get('file_name') == self.schema_filename else False)
