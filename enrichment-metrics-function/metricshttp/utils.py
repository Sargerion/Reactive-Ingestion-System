server = 'tcp:<server>.database.windows.net'
database = '<database>'
driver = '{ODBC Driver 17 for SQL Server}'
metrics_table_name = '<table>'
metrics_table_schema = '(rows_count_delta BIGINT, ingestion_date DATE, ingestion_time TIME, all_ingested_rows BIGINT)'
query_create_metrics_table = f"IF NOT EXISTS (SELECT * FROM sysobjects WHERE NAME='{metrics_table_name}') CREATE TABLE {metrics_table_name} {metrics_table_schema};"
query_insert_metrics = f"INSERT INTO {metrics_table_name} VALUES (?, ?, ?, ?);"
query_select_previous_metrics = f"SELECT TOP (1) rows_count_delta, ingestion_date, ingestion_time, all_ingested_rows FROM {metrics_table_name} ORDER BY ingestion_date DESC, ingestion_time DESC;"
query_select_current_ingestion_state = "SELECT COUNT(*) FROM {}"
load_threshold = 1000

def build_logic_app_payload(current_rows_count, previous_pipeline_metrics, ingestion_date, ingestion_time):
    return {
        "rows_count_delta" : current_rows_count[0] - previous_pipeline_metrics[3],
        "ingestion_date" : str(ingestion_date),
        "ingestion_time" : ingestion_time,
        "current_ingestion_table_state" : current_rows_count[0]
    }


def build_connection_string(driver, server, database):
    return ("Driver="
            + driver
            + ";Server="
            + server
            + ";PORT=1433;Database="
            + database
            + ";Authentication=ActiveDirectoryMsi")