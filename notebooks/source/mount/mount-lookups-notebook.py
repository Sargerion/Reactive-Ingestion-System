# Databricks notebook source
LOOKUPS_STORAGE_ACCOUNT = dbutils.widgets.get('prepared-trades-assets-st-acc')
LOOKUPS_STORAGE_CONTAINER = dbutils.widgets.get('prepared-trades-assets-container')
LOOKUPS_MOUNT = f'/mnt/{LOOKUPS_STORAGE_CONTAINER}'

# COMMAND ----------

try:
    dbutils.fs.mount(
        source = f'wasbs://{LOOKUPS_STORAGE_CONTAINER}@{LOOKUPS_STORAGE_ACCOUNT}.blob.core.windows.net/',
        mount_point = LOOKUPS_MOUNT,
        extra_configs = {f'fs.azure.sas.{LOOKUPS_STORAGE_CONTAINER}.{LOOKUPS_STORAGE_ACCOUNT}.blob.core.windows.net' : dbutils.secrets.get(scope = 'ris-kv-scope', key = 'ris-lookups-storage-account-sas')}
    )
except:
    print('Lookups already mounted.')

# COMMAND ----------

LOOKUPS_PATH = LOOKUPS_MOUNT + '/' + dbutils.widgets.get('prepared-trades-assets-path')