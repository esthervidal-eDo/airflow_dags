from airflow.sensors.external_task_sensor import ExternalTaskSensor

DEFAULT_EXTERNAL_TASK_ID = None
DEFAULT_ALLOWED_STATES = None
DEFAULT_EXECUTION_DELTA = None
DEFAULT_EXECUTION_DATE = None
DEFAULT_CHECK_EXISTENCE = True


def operator(dag,
             task_id,
             external_dag_id,
             external_task_id=DEFAULT_EXTERNAL_TASK_ID,
             allowed_states=DEFAULT_ALLOWED_STATES,
             execution_delta=DEFAULT_EXECUTION_DELTA,
             execution_date_fn=DEFAULT_EXECUTION_DATE,
             check_existence=DEFAULT_EXECUTION_DATE,
             *args, **kwargs):
    return ExternalTaskSensor(
        dag=dag,
        task_id=task_id,
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        allowed_states=allowed_states,
        execution_delta=execution_delta,
        execution_date_fn=execution_date_fn,
        check_existence=check_existence
    )
