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
# from apache_beam.options.value_provider import ValueProvider
from apache_beam.options.value_provider import StaticValueProvider
from apache_beam.io.filesystem import CompressionTypes


list_fields = ['attributed_touch_type','attributed_touch_time','install_time','event_time','event_name','event_value','event_revenue','event_revenue_currency','event_revenue_usd','af_cost_model','af_cost_value','af_cost_currency','event_source','is_receipt_validated','af_prt','media_source','af_channel','af_keywords','install_app_store','campaign','af_c_id','af_adset','af_adset_id','af_ad','af_ad_id','af_ad_type','af_siteid','af_sub_siteid','af_sub1','af_sub2','af_sub3','af_sub4','af_sub5','contributor_1_touch_type','contributor_1_touch_time','contributor_1_af_prt','contributor_1_match_type','contributor_1_media_source','contributor_1_campaign','contributor_2_touch_type','contributor_2_touch_time','contributor_2_af_prt','contributor_2_media_source','contributor_2_campaign','contributor_2_match_type','contributor_3_touch_type','contributor_3_touch_time','contributor_3_af_prt','contributor_3_media_source','contributor_3_campaign','contributor_3_match_type','region','country_code','state','city','postal_code','dma','ip','wifi','operator','carrier','language','appsflyer_id','customer_user_id','android_id','advertising_id','imei','idfa','idfv','amazon_aid','device_type','device_category','platform','os_version','app_version','sdk_version','app_id','app_name','bundle_id','is_retargeting','retargeting_conversion_type','is_primary_attribution','af_attribution_lookback','af_reengagement_window','match_type','user_agent','http_referrer','original_url','gp_referrer','gp_click_time','gp_install_begin','gp_broadcast_referrer','custom_data','network_account_id','keyword_match_type','blocked_reason','blocked_reason_value','blocked_reason_rule','blocked_sub_reason','af_web_id','web_event_type','media_type','pid','utm_source','utm_medium','utm_term','utm_content','utm_campaign','device_download_time','deeplink_url','oaid','media_channel','event_url','utm_id','ad_unit','segment','placement','mediation_network','impressions','monetization_network', 'conversion_id']
# dataset_name = 'test'
# task_name = 'temp_appsflyer'


class UserOptions(PipelineOptions):
    @classmethod
    def _add_argparse_args(cls, parser):
        parser.add_value_provider_argument(
            '--input',
            default='gs://temp_appsflyer_data_service/1d72-acc-swhSmYxE-1d72/data-locker-hourly/*/*/*/*.gz',
            type=str,
            required=False,
            help='path of input file')
        parser.add_value_provider_argument(
            '--output',
            default='baemin-vietnam:appsflyer.events',
            type=str,
            required=False,
            help='bigquery table spec')


def generate_schema_from_fields(list_fields):
    from apache_beam.io.gcp.internal.clients.bigquery import TableSchema, TableFieldSchema

    df_schema = TableSchema()
    for _ in list_fields:
        temp_df_schema = TableFieldSchema()
        temp_df_schema.name = _
        temp_df_schema.type = 'STRING'
        temp_df_schema.mode = 'NULLABLE'
        df_schema.fields.append(temp_df_schema)

    temp_df_schema = TableFieldSchema()
    temp_df_schema.name = 'event_date'
    temp_df_schema.type = 'DATE'
    temp_df_schema.mode = 'NULLABLE'
    df_schema.fields.append(temp_df_schema)

    temp_df_schema = TableFieldSchema()
    temp_df_schema.name = 'ds_uuid'
    temp_df_schema.type = 'STRING'
    temp_df_schema.mode = 'NULLABLE'
    df_schema.fields.append(temp_df_schema)

    return df_schema


def setdiff_sorted(array1, array2, assume_unique=True):
    import numpy as np

    ans = np.setdiff1d(array1,array2,assume_unique).tolist()
    if assume_unique:
        return sorted(ans)
    return ans


# def update_table_schema(new_df_cols, dataset_name, task_name):
#     from google.cloud import bigquery
#     from google.oauth2 import service_account

#     bigquery_client = bigquery.Client.from_service_account_json("data_service.json")
#     dataset_ref = bigquery_client.dataset(dataset_name)
#     table_ref = dataset_ref.table(task_name)

#     table = bigquery_client.get_table(table_ref)
#     new_schema = list(table.schema)

#     list_schema = []
#     i = 0
#     for i in range (0, len(new_schema)):
#         list_schema.append(str(new_schema[i]).split(',')[0][13:-1])

#     added_fields = setdiff_sorted(new_df_cols, list_schema)

#     for field in added_fields:
#         new_schema.append(bigquery.SchemaField(f'{field}', 'STRING'))

#     table.schema = new_schema
#     table = bigquery_client.update_table(table, ['schema'])


# def update_file_schema():



def get_csv_reader(readable_file_metadata):
    import io
    import gzip
    import csv
    import sys
    from apache_beam.io.filesystems import FileSystems
    from apache_beam.io.filesystem import CompressionTypes
    csv.field_size_limit(sys.maxsize)

    with FileSystems.open(readable_file_metadata.path, compression_type=CompressionTypes.UNCOMPRESSED) as fopen:
        decompressed_str = io.StringIO(gzip.decompress(fopen.read()).decode('utf-8'))
        reader = csv.DictReader(decompressed_str)
        return reader


def convert_to_dict(element, list_fields):
    import datetime as dt
    import logging
    import numpy as np
    import uuid

    list_keys = list(element.keys())
    new_col = setdiff_sorted(list_keys, list_fields)
    if len(new_col) > 0:
        logging.critical(f'list of new columns to be added: {new_col}')
        for col in new_col:
            element.pop(col)

    # if len(new_col) > 0:
    #     update_table_schema(list_keys, dataset_name, task_name)
    try:
        # test = dt.datetime.strptime(element['event_time'], '%Y-%m-%d %H:%M:%S').date()
        test = dt.datetime.strptime(element['event_time'][:10], '%Y-%m-%d')
        element['event_date'] = element['event_time'][:10]
        element.move_to_end('event_date', last = False)
    except:
        logging.info('ERROR occured while parsing event_date')
        element['event_date'] = '1999-01-01'
        element.move_to_end('event_date', last = False)

    current_timestamp = str(dt.datetime.now().timestamp())[:10]
    ds_uuid = current_timestamp + '-'+ str(uuid.uuid4())
    element['ds_uuid'] = ds_uuid
    
    return element    

def main():
   
    df_schema = generate_schema_from_fields(list_fields)

    pipeline_options = PipelineOptions()
    user_options = pipeline_options.view_as(UserOptions)
    # cloud_options = pipeline_options.view_as(GoogleCloudOptions)
    pipeline_options.view_as(SetupOptions).save_main_session = True
    pipeline_options.view_as(WorkerOptions).autoscaling_algorithm = 'THROUGHPUT_BASED'
    pipeline_options.view_as(WorkerOptions).max_num_workers = 50
    # pipeline_options.view_as(WorkerOptions).num_workers = 20

    p = beam.Pipeline(options=pipeline_options)

    lines = (
        p
        | fileio.MatchFiles('gs://temp_appsflyer_data_service/1d72-acc-swhSmYxE-1d72/data-locker-hourly/*/*/*/*.gz')
        | beam.Reshuffle()
        | beam.FlatMap(get_csv_reader)
        | beam.Map(convert_to_dict, list_fields)
        | beam.io.WriteToBigQuery('baemin-vietnam:appsflyer.events', schema=df_schema, write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND, create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED)
    )

    
    result = p.run()
    result.wait_until_finish()



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    main()

