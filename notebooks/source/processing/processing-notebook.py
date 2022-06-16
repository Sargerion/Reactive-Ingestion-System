# Databricks notebook source
for s in spark.streams.active:
    s.stop()

# COMMAND ----------

spark.conf.set('spark.sql.shuffle.partitions', 4)

# COMMAND ----------

EVENT_HUB_CONNECTION = dbutils.secrets.get(scope = 'ris-kv-scope', key = 'ris-load-trades-hub-read-sas')
ehReadConf = {'eventhubs.connectionString' : sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(EVENT_HUB_CONNECTION)}

# COMMAND ----------

coinStream = (
    spark
    .readStream
    .format('eventhubs')
    .options(**ehReadConf)
    .load()
)

# COMMAND ----------

from pyspark.sql.types import StructField, StructType, StringType, LongType, DoubleType
from pyspark.sql.functions import col, from_json


event_schema = StructType([
    StructField('exchange', StringType(), True),
    StructField('base', StringType(), True),
    StructField('quote', StringType(), True),
    StructField('direction', StringType(), True),
    StructField('price', DoubleType(), True),
    StructField('volume', DoubleType(), True),
    StructField('timestamp', LongType(), True),
    StructField('priceUsd', DoubleType(), True)
])

parsedCoinStream = coinStream.select(from_json(col('body').cast('string'), event_schema).alias('coin_data'))

# COMMAND ----------

from pyspark.sql.functions import to_utc_timestamp, from_unixtime, concat_ws, substring


flatCoinStream = (
    parsedCoinStream
    .select(
        col('coin_data.exchange').alias('exchange'),
        col('coin_data.base').alias('base'),
        col('coin_data.quote').alias('quote'),
        col('coin_data.direction').alias('direction'),
        col('coin_data.price').alias('price'),
        col('coin_data.volume').alias('volume'),
        to_utc_timestamp(
            concat_ws('.', from_unixtime((col('coin_data.timestamp') / 1000), 'yyyy-MM-dd HH:mm:ss'), substring(col('coin_data.timestamp'), -3, 3)), 'UTC-3'
        ).alias('timestamp'),
        col('coin_data.priceUsd').alias('priceUsd')
    )
    .dropDuplicates()
)



# COMMAND ----------

# MAGIC %run ../mount/mount-lookups-notebook

# COMMAND ----------

lookupDf = (
    spark
    .read
    .parquet(LOOKUPS_PATH)
)

# COMMAND ----------

from pyspark.sql.functions import broadcast

enrichedStream = (
    flatCoinStream
    .join(
        broadcast(lookupDf), flatCoinStream.base == lookupDf.id
    )
    .select(
        col('id')
       ,col('name')
       ,col('symbol')
       ,col('exchange')
       ,col('quote')
       ,col('direction')
       ,col('price')
       ,col('volume')
       ,col('timestamp')
       ,col('explorer')
       ,col('marketCapUsd')
       ,col('maxSupply')
       ,col('rank')
       ,col('changePercent24Hr')
       ,col('volumeUsd24Hr')
       ,col('vwap24Hr')
    )
)

# COMMAND ----------

from datetime import date, datetime


def json_serial(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError ('Type %s not serializable' % type(obj))

    
def build_json_data(row):
    return [{
        'id' : row['id'],
        'name' : row['name'],
        'symbol' : row['symbol'],
        'exchange' : row['exchange'],
        'quote' : row['quote'],
        'direction' : row['direction'],
        'price' : row['price'],
        'volume' : row['volume'],
        'timestamp' : row['timestamp'],
        'explorer' : row['explorer'],
        'marketCapUsd' : row['marketCapUsd'],
        'maxSupply' : row['maxSupply'],
        'rank' : row['rank'],
        'changePercent24Hr' : row['changePercent24Hr'],
        'volumeUsd24Hr' : row['volumeUsd24Hr'],
        'vwap24Hr' : row['vwap24Hr']
    }]

# COMMAND ----------

import requests
import json


POWER_BI_URL = dbutils.secrets.get(scope = 'ris-kv-scope', key = 'ris-power-bi-trades-dataset-url')


def process_to_bi(row):
    json_data = build_json_data(row)
    res = requests.post(POWER_BI_URL, data=json.dumps(json_data, default=json_serial))
    print(res.status_code)

# COMMAND ----------

JDBC_URL = f"jdbc:sqlserver://<server>.database.windows.net:1433;database=<database>;user=<user>@<server>;password={dbutils.secrets.get(scope = 'ris-kv-scope', key = 'ris-azure-sql-db-password')};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"

# COMMAND ----------

def process_to_bi_and_db(source_df, batch_id):
    source_df.persist()
    source_df.foreach(process_to_bi)
    (
        source_df.write
        .format('jdbc')
        .option('url', JDBC_URL)
        .option('dbtable', '<table>')
        .mode('append')
        .save()
    )
    source_df.unpersist()

# COMMAND ----------

# MAGIC %run ../mount/mount-checkpoints-notebook

# COMMAND ----------

(
    enrichedStream.writeStream
    .option('checkpointLocation', CHECKPOINTS_PATH)
    .foreachBatch(process_to_bi_and_db)
    .queryName('trades-stream')
    .start()
)