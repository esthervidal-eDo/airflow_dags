from datetime import timedelta

from airflow.contrib.sensors.gcs_sensor import  GoogleCloudStorageObjectUpdatedSensor
from airflow.models import Variable

from edo.dagger.utils.utils import remove_gs_from_bucket

DEFAULT_GOOGLE_CLOUD_CONN_ID = 'google_cloud_default'
DEFAULT_DELEGATE_TO = None
DEFAULT_POKE_INTERVAL = 60
DEFAULT_TIMEOUT = 60 * 60 * 24 * 7
DEFAULT_SOFT_FAIL = False
DEFAULT_MODE = 'poke'
DEFAULT_TS_FUNCTION = lambda context: context.get("execution_date") + timedelta(days=1)


def operator(dag, task_id, object_name,
                              bucket_from_airflow, params,
                              google_cloud_conn_id=DEFAULT_GOOGLE_CLOUD_CONN_ID,
                              delegate_to=DEFAULT_DELEGATE_TO,
                              poke_interval=DEFAULT_POKE_INTERVAL,
                              timeout=DEFAULT_TIMEOUT,
                              soft_fail=DEFAULT_SOFT_FAIL,
                              ts_func=DEFAULT_TS_FUNCTION,
                              mode=DEFAULT_MODE,
                              *args, **kwargs):
    bucket = remove_gs_from_bucket(Variable.get(bucket_from_airflow))
    return GoogleCloudStorageObjectUpdatedSensor(
        dag=dag, task_id=task_id,
        params=params,
        bucket=bucket,
        object=object_name,
        google_cloud_conn_id=google_cloud_conn_id,
        delegate_to=delegate_to,
        poke_interval=poke_interval,
        ts_func=ts_func,
        timeout=timeout,
        soft_fail=soft_fail,
        mode=mode
    )
