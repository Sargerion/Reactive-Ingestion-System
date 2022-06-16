# Databricks notebook source
CHECKPOINTS_STORAGE_ACCOUNT = dbutils.secrets.get(scope = 'ris-kv-scope', key = 'ris-pipelines-checkpoints-storage-account-name')
CHECKPOINT_STORAGE_CONTAINER = dbutils.secrets.get(scope = 'ris-kv-scope', key = 'ris-pipelines-checkpoints-storage-container')
CHECKPOINTS_MOUNT = f'/mnt/{CHECKPOINT_STORAGE_CONTAINER}'

# COMMAND ----------

try:
    dbutils.fs.mount(
        source = f'wasbs://{CHECKPOINT_STORAGE_CONTAINER}@{CHECKPOINTS_STORAGE_ACCOUNT}.blob.core.windows.net/',
        mount_point = CHECKPOINTS_MOUNT,
        extra_configs = {f'fs.azure.sas.{CHECKPOINT_STORAGE_CONTAINER}.{CHECKPOINTS_STORAGE_ACCOUNT}.blob.core.windows.net' : dbutils.secrets.get(scope = 'ris-kv-scope', key = 'ris-pipelines-checkpoints-storage-account-sas')}
    )
except:
    print('Checkpoints container already mounted.')

# COMMAND ----------

CHECKPOINTS_PATH = CHECKPOINTS_MOUNT + '/checkpoints'