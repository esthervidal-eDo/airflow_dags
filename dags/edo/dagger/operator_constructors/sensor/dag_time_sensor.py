from airflow import AirflowException
from airflow.sensors.time_sensor import TimeSensor
from datetime import datetime

def operator(dag,
             task_id,
             target_time,
             params,
             *args, **kwargs):

    variable_name = get_param_variable("{{ ?params.([0-z_]+) }}", target_time)
    if variable_name is None:
        raise AirflowException("TimeSensor's param 'target_time' bad formatted: {target_time}".format(target_time=target_time))

    variable_value = params.get(variable_name, None)
    if variable_value is None:
        raise AirflowException("TimeSensor's param 'target_time' value not found: {target_time}".format(target_time=target_time))

    return TimeSensor(
        dag=dag,
        task_id=task_id,
        target_time=datetime.strptime(variable_value, '%H:%M:%S').time(),
        params=params
    )

def get_param_variable(regex, param_value):
    import re
    found = re.search(regex, param_value)
    if found:
        return found.group(1)
    else:
        return None
