from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models import Variable
from google.cloud.bigquery import WriteDisposition

from edo.dagger.utils.utils import remove_gs_from_bucket

DEFAULT_SCHEMA_FIELDS = None
DEFAULT_SCHEMA_OBJECT = None
DEFAULT_SOURCE_FORMAT = 'PARQUET'
DEFAULT_COMPRESSION = 'NONE'
DEFAULT_CREATE_DISPOSITION = 'CREATE_IF_NEEDED'
DEFAULT_SKIP_LEADING_ROWS = 0
DEFAULT_WRITE_DISPOSITION = WriteDisposition.WRITE_EMPTY
DEFAULT_FIELD_DELIMITER = ','
DEFAULT_MAX_BAD_RECORDS = 0
DEFAULT_QUOTE_CHARACTER = None
DEFAULT_IGNORE_UNKNOWN_VALUES = False
DEFAULT_ALLOW_QUOTED_NEWLINES = False
DEFAULT_ALLOW_JAGGED_ROWS = False
DEFAULT_MAX_ID_KEY = None
DEFAULT_BIGQUERY_CONN_ID = 'bigquery_default'
DEFAULT_GOOGLE_CLOUD_STORAGE_CONN_ID = 'google_cloud_default'
DEFAULT_DELEGATE_TO = None
DEFAULT_SCHEMA_UPDATE_OPTIONS = ()
DEFAULT_SRC_FMT_CONFIGS = {}
DEFAULT_EXTERNAL_TABLE = False
DEFAULT_TIME_PARTITIONING = {}
DEFAULT_CLUSTER_FIELDS = None
DEFAULT_AUTODETECT = False


def operator(
        dag,
        task_id,
        bucket_from_airflow,
        source_objects,
        destination_project_dataset_table,
        schema_fields=DEFAULT_SCHEMA_FIELDS,
        schema_object=DEFAULT_SCHEMA_OBJECT,
        source_format=DEFAULT_SOURCE_FORMAT,
        compression=DEFAULT_COMPRESSION,
        create_disposition=DEFAULT_CREATE_DISPOSITION,
        skip_leading_rows=DEFAULT_SKIP_LEADING_ROWS,
        write_disposition=DEFAULT_WRITE_DISPOSITION,
        field_delimiter=DEFAULT_FIELD_DELIMITER,
        max_bad_records=DEFAULT_MAX_BAD_RECORDS,
        quote_character=DEFAULT_QUOTE_CHARACTER,
        ignore_unknown_values=DEFAULT_IGNORE_UNKNOWN_VALUES,
        allow_quoted_newlines=DEFAULT_ALLOW_QUOTED_NEWLINES,
        allow_jagged_rows=DEFAULT_ALLOW_JAGGED_ROWS,
        max_id_key=DEFAULT_MAX_ID_KEY,
        bigquery_conn_id=DEFAULT_BIGQUERY_CONN_ID,
        google_cloud_storage_conn_id=DEFAULT_GOOGLE_CLOUD_STORAGE_CONN_ID,
        delegate_to=DEFAULT_DELEGATE_TO,
        schema_update_options=DEFAULT_SCHEMA_UPDATE_OPTIONS,
        src_fmt_configs=DEFAULT_SRC_FMT_CONFIGS,
        external_table=DEFAULT_EXTERNAL_TABLE,
        time_partitioning=DEFAULT_TIME_PARTITIONING,
        cluster_fields=DEFAULT_CLUSTER_FIELDS,
        autodetect=DEFAULT_AUTODETECT,
        *args, **kwargs):
    bucket = remove_gs_from_bucket(Variable.get(bucket_from_airflow))
    return GoogleCloudStorageToBigQueryOperator(
        dag=dag,
        task_id=task_id,
        bucket=bucket,
        source_objects=source_objects,
        destination_project_dataset_table=destination_project_dataset_table,
        schema_fields=schema_fields,
        schema_object=schema_object,
        source_format=source_format,
        compression=compression,
        create_disposition=create_disposition,
        skip_leading_rows=skip_leading_rows,
        write_disposition=write_disposition,
        field_delimiter=field_delimiter,
        max_bad_records=max_bad_records,
        quote_character=quote_character,
        ignore_unknown_values=ignore_unknown_values,
        allow_quoted_newlines=allow_quoted_newlines,
        allow_jagged_rows=allow_jagged_rows,
        max_id_key=max_id_key,
        bigquery_conn_id=bigquery_conn_id,
        google_cloud_storage_conn_id=google_cloud_storage_conn_id,
        delegate_to=delegate_to,
        schema_update_options=schema_update_options,
        src_fmt_configs=src_fmt_configs,
        external_table=external_table,
        time_partitioning=time_partitioning,
        cluster_fields=cluster_fields,
        autodetect=autodetect
    )
