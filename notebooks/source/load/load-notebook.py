# Databricks notebook source
# MAGIC %run ../reactive-adf/trigger-pipeline-notebook

# COMMAND ----------

COINCAP_WS = 'wss://ws.coincap.io/trades/binance'
LOAD_EVENT_HUB_CONNECTION = dbutils.secrets.get(scope = 'ris-kv-scope', key = 'ris-load-trades-hub-write-sas')
LOAD_EVENT_HUB_NAME = dbutils.secrets.get(scope = 'ris-kv-scope', key = 'ris-load-hub-name')

# COMMAND ----------

import websocket
from azure.eventhub import EventHubProducerClient, EventData
from azure.eventhub.exceptions import EventHubError


def on_message(ws, message):
    try:
        producer = EventHubProducerClient.from_connection_string(
            conn_str=LOAD_EVENT_HUB_CONNECTION,
            eventhub_name=LOAD_EVENT_HUB_NAME
        )
        with producer:
            producer.send_batch([EventData(message)])
    except EventHubError as eh_err:
        print("EventHub error: ", eh_err)
    

websocket.enableTrace(True)
ws = websocket.WebSocketApp(
    COINCAP_WS,
    on_message=on_message
)
send_adf_trigger(subject='Trades')
ws.run_forever()