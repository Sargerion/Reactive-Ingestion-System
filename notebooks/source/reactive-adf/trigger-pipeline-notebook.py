# Databricks notebook source
ADF_TRIGGER_TOPIC_ENDPOINT = dbutils.secrets.get(scope = 'ris-kv-scope', key = 'ris-adf-trigger-topic-endpoint')
ADF_TRIGGER_TOPIC_KEY = dbutils.secrets.get(scope = 'ris-kv-scope', key = 'ris-adf-trigger-topic-key')

# COMMAND ----------

import datetime
import uuid
from azure.core.credentials import AzureKeyCredential
from azure.eventgrid import EventGridPublisherClient, EventGridEvent


def build_event(subject):
    return EventGridEvent(
        id=uuid.uuid4(),
        subject=f'ADF_Trigger_{subject}',
        data={
            'trigger_payload': f'trigger_enrichment_at:{datetime.datetime.now()}'
        },
        event_type='ADF_Event',
        event_time=datetime.datetime.now(),
        data_version=1.0
    )
    

def send_adf_trigger(subject):
    credential = AzureKeyCredential(ADF_TRIGGER_TOPIC_KEY)
    event_grid_client = EventGridPublisherClient(ADF_TRIGGER_TOPIC_ENDPOINT, credential)
    event_grid_client.send(build_event(subject))
    print('Published event to Event Grid.')