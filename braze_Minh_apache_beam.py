from __future__ import absolute_import

import argparse
import logging
import re

from past.builtins import unicode

import apache_beam as beam
from apache_beam.io import fileio
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.metrics import Metrics
from apache_beam.metrics.metric import MetricsFilter
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.options.pipeline_options import SetupOptions, WorkerOptions
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.io.filesystem import CompressionTypes


class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--input',
            default='gs://braze_user_data_service/*/*/*/*/*.gz',
            type=str,
            required=False,
            help='path of input file')
        parser.add_value_provider_argument(
            '--output',
            type=str,
            required=False,
            help='bigquery table spec')
        # parser.add_value_provider_argument(
        #     '--secret_output',
        #     type=str,
        #     required=False,
        #     help='bigquery table spec')
        # parser.add_value_provider_argument(
        #     '--schema_table',
        #     default="baemin-vietnam.braze.user_profile_20200616",
        #     type=str,
        #     required=False,
        #     help='bigquery table spec')


def hash_json_data(row):

    import hashlib
    col_hash = ['email', 'addressDetail', 'first_name', 'address', 'phone', 'email_subscribe']
    for col in col_hash:
        if col in row.keys(): 
            row[col]= row[col] + "hEh$0"
            row[col] = hashlib.sha256(str(row[col]).encode('utf-8')).hexdigest().upper()

        if 'push_tokens' in row.keys(): 
            for push_row in row['push_tokens']:
                if 'token' in push_row.keys():
                    push_row['token']= push_row['token'] + "hEh$0"
                    push_row['token'] = hashlib.sha256(str(push_row['token']).encode('utf-8')).hexdigest().upper()
    return row


def copy_bq_schema(bq_schema):
    from apache_beam.io.gcp.internal.clients import bigquery
    from apache_beam.io.gcp.internal.clients.bigquery import TableSchema, TableFieldSchema

    df_schema = bigquery.TableSchema()

    for schema in bq_schema:
        temp_df_schema = TableFieldSchema()
        if schema.fields == ():
            temp_df_schema.name = schema.name
            if temp_df_schema.name == 'af_c_id':
                temp_df_schema.type = 'STRING'
            else:
                temp_df_schema.type = schema.field_type
            # temp_df_schema.type = 'STRING'
            temp_df_schema.mode = schema.mode
            temp_df_schema.description = schema.description
            temp_df_schema.fields = schema.fields
            df_schema.fields.append(temp_df_schema)
        else:
            sub_df_schema = copy_bq_schema(schema.fields)
            temp_df_schema.fields.extend(sub_df_schema.fields)
            temp_df_schema.name = schema.name
            temp_df_schema.type = schema.field_type
#             if schema.field_type != 'RECORD':
#                 temp_df_schema.type = 'STRING'
#             else:
#                 temp_df_schema.type = schema.field_type
            temp_df_schema.mode = schema.mode
            temp_df_schema.description = schema.description
            df_schema.fields.append(temp_df_schema)

    return df_schema


def load_json(data):
    import json
    return json.loads(data)


def fix_col_name(data):
    data = { k.replace(' ', '_'): v for k, v in data.items() }
    data = { k.replace('-', '_'): v for k, v in data.items() }
    return data


def fix_timestamp(data):
    if 'last_received' in data:
        data['last_received'] = data['last_received'].str.replace('T', ' ')
        data['last_received'] = data['last_received'].str.replace('Z', '')
    return data


def convert_data_types(data):
    import json
    data = json.loads(json.dumps(data), parse_float=str, parse_constant=str, parse_int=str)
    return data


def reconstruct_data(row):
    import datetime as dt
    specified_col = ['deviceUniqueId', 'amplitudeDeviceId', 'cloneAppPackages', 'country', 'locality', 'subLocality', 'subAdminArea', 'feature', 'address', 'addressDetail', 'lat', 'lng', 'af_status', 'is_first_launch', 'install_time', 'af_click_lookback', 'media_source', 'click_time', 'campaign', 'adset', 'af_channel', 'adgroup_id', 'is_paid', 'is_mobile_data_terms_signed', 'is_fb', 'adgroup', 'adset_id', 'campaign_id', 'agency', 'ad_id', 'last_reviewed_order_id', 'orig_cost', 'iscache', 'external_account_id', 'af_reengagement_window', 'af_viewthrough_lookback', 'af_prt', 'af_c_id', 'af_ad_type', 'ad_event_id', 'network', 'click-timestamp']
    if 'custom_attributes' in row.keys():
        for add_row in specified_col:
            if add_row in row['custom_attributes'].keys():
                if add_row == 'click-timestamp':
                    value = int(row['custom_attributes']['click-timestamp'])
                    value = dt.datetime.fromtimestamp(value/1000.0)
                    value = dt.datetime.strftime(value, "%Y-%m-%d %H:%M:%S.%f")
                    row['click_timestamp'] = value
                else:
                    value = row['custom_attributes'][add_row]
                    row[add_row] = str(value)
        row['custom_attributes']= str(row['custom_attributes'])
    if 'campaigns_received' in row.keys():
        for campaign in row['campaigns_received']:
            if 'multiple_converted' in campaign.keys():
                campaign['multiple_converted'] = str(campaign['multiple_converted'])

    if 'canvases_received' in row.keys():
        for canvas in row['canvases_received']:
            canvas_key = ['last_received_message','last_entered_control_at']
            for key in canvas_key:
                if canvas[key] == None:
                    del canvas[key]
            if 'in_control' in canvas.keys() and 'is_in_control' in canvas.keys():
                if canvas['is_in_control']== False:
                    del canvas['in_control']
            if 'steps_received' in canvas.keys() and len(canvas['steps_received'])==0:
                del canvas['steps_received']

    return row


def main():

    from google.cloud import bigquery
    import datetime as dt
    client = bigquery.Client.from_service_account_json("data_service.json")

    pipeline_options = PipelineOptions()
    user_options = pipeline_options.view_as(UserOptions)
    # cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    # pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(WorkerOptions).autoscaling_algorithm = 'THROUGHPUT_BASED'
    pipeline_options.view_as(WorkerOptions).max_num_workers = 100
    pipeline_options.view_as(WorkerOptions).num_workers = 20

    table_id = 'baemin-vietnam.braze.user_profile_20200616'
    table = client.get_table(table_id)  # Make an API request.
    original_schema = table.schema
    df_schema = copy_bq_schema(original_schema)

    p = beam.Pipeline(options=pipeline_options)

    lines = (
        p
        # | ReadFromText('gs://braze_user_data_service/segment-export/024159a9-5faa-4287-9fa2-8c3163179957/2020-06-02/*/*.gz', compression_type=CompressionTypes.AUTO)
        | ReadFromText("gs://braze_user_data_service/*/*/*/*/*.gz", compression_type=CompressionTypes.AUTO)
        | beam.Map(load_json)
        | beam.Map(reconstruct_data)
        | beam.Map(hash_json_data)
        | 'Write to braze dataset' >> beam.io.WriteToBigQuery(user_options.output, schema=df_schema, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    #     | 'write to BigQuery' >> beam.io.Write(beam.io.BigQuerySink(project='baemin-vietnam',dataset='test',table='df_pipeline', create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND))
    )

    result = p.run()
    result.wait_until_finish()



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()

# run command line 
# python test.py --project "baemin-vietnam" --job_name "appsflyer-test" --temp_location "gs://temp_data_service/tmp" --region us-central1 --save_main_session true

# python main.py --runner DataflowRunner --project “baemin-vietnam” --job_name “appsflyer-full-reshuffle” --temp_location “gs://temp_data_service/tmp” --region us-central1 --save_main_session true
# 9:34
# --output baemin-vietnam:braze.user_profile_20200714


