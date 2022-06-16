import logging
import os
import sys
import azure.functions as func
import pyodbc
from datetime import datetime
import requests
import json


dir_path = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(0, dir_path)
import utils    


def main(req: func.HttpRequest) -> func.HttpResponse:
    req_body = json.loads(req.get_body().decode('utf-8'))
    ingestion_table = req_body['ingestion_table']
    ingestion_time = req_body['ingestion_time']
    ingestion_date = datetime.now().date()
    try:
        with pyodbc.connect(
              utils.build_connection_string(utils.driver, utils.server, utils.database)
            , autocommit=False
        ) as conn:
            logging.info("Successful connection to database")
            with conn.cursor() as cursor:
                cursor.execute(utils.query_create_metrics_table)

                cursor.execute(utils.query_select_previous_metrics)
                previous_pipeline_metrics = cursor.fetchone()

                cursor.execute(utils.query_select_current_ingestion_state.format(ingestion_table))
                current_rows_count = cursor.fetchone()

                if previous_pipeline_metrics is not None:
                    loaded_rows_delta = current_rows_count[0] - previous_pipeline_metrics[3]
                    all_ingested_rows = current_rows_count[0]
                    cursor.execute(utils.query_insert_metrics, loaded_rows_delta, ingestion_date, ingestion_time, all_ingested_rows)
                    if loaded_rows_delta < utils.load_threshold:
                        logic_app_url = req_body['logic_app_url']
                        requests.post(logic_app_url, json=utils.build_logic_app_payload(current_rows_count, previous_pipeline_metrics, ingestion_date, ingestion_time))
                        return func.HttpResponse('Suspicious rows count = {}, alert was send.'.format(loaded_rows_delta))
                    else:
                        return func.HttpResponse('Metrics updated successfully, rows loaded -> {}.'.format(loaded_rows_delta))
                else:
                    cursor.execute(utils.query_insert_metrics, 0, ingestion_date, ingestion_time, all_ingested_rows)
                    return func.HttpResponse('Metrics loaded successfully, rows added -> {}.'.format(all_ingested_rows))
    except Exception as e:
        return func.HttpResponse(str(e))
