from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.decorators import apply_defaults

DEFAULT_USE_LEGACY_SQL=False
DEFAULT_LOCATION="EU"
DEFAULT_POKE_INTERVAL=60

class BigQueryPartitionSensor(SqlSensor):

    @apply_defaults
    def __init__(self, use_legacy_sql, location, conn_id, table_def, partition_def, poke_interval, *args, **kwargs):
        super(BigQueryPartitionSensor, self).__init__(conn_id=conn_id, sql="SELECT 1", poke_interval=poke_interval, *args, **kwargs)
        self.use_legacy_sql = use_legacy_sql
        self.location = location
        self.table_def = table_def
        self.partition_def = partition_def

    def _get_hook(self):
        hook = super()._get_hook()
        hook.use_legacy_sql = self.use_legacy_sql
        hook.location = self.location
        return hook

    def poke(self, context):
        self.sql = self.build_sql(context)

        return super().poke(context)

    def build_sql(self, context):
        QUERY_BY_ID='''select COUNT(1)>0 from `{project_id}.{dataset_id}.{table_id}` where {partition_id} = '{partition_value}' '''

        if self.partition_def.get("partition_value") is None:
            self.partition_def["partition_value"] = context.get("ds")

        query = QUERY_BY_ID.format(**self.table_def, **self.partition_def)
        print('Built query: {query}'.format(query=query))

        return query


def operator(dag, task_id,
              params,
              conn_id,
              table_def,
              partition_def,
              use_legacy_sql=DEFAULT_USE_LEGACY_SQL,
              location=DEFAULT_LOCATION,
              poke_interval=DEFAULT_POKE_INTERVAL,
              *args, **kwargs):

    print('Params dictionaries: {table_def} - {partition_def}'.format(table_def=table_def,partition_def=partition_def))

    return BigQueryPartitionSensor(
        use_legacy_sql=use_legacy_sql,
        location=location,
        conn_id=conn_id,
        table_def=table_def,
        partition_def=partition_def,
        dag=dag,
        task_id=task_id,
        poke_interval=poke_interval
   )
