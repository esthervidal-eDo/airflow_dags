from datetime import timedelta

import pendulum
from airflow import AirflowException
from airflow.models import BaseOperator


class WaitOperator(BaseOperator):
    def __init__(self, waiting_time, *args, **kwargs):
        super(WaitOperator, self).__init__(*args, **kwargs)
        if (isinstance(waiting_time, dict)):
            self.waiting_time = timedelta(**waiting_time)
        elif (isinstance(waiting_time, timedelta)):
            self.waiting_time = waiting_time
        else:
            raise AirflowException(self.__class__, "Wrong input. accepted __init__s are: dict, timedelta.")
        self.retry_delay = self.waiting_time
        self.retries = 1

    """
    Fail if not enough time has passed.
    """

    def execute(self, context):
        time_format = "%Y-%m-%d %H:%M:%S"
        desired_timestamp = context.get("next_execution_date") + self.waiting_time
        if pendulum.now() < desired_timestamp:
            print("The value of execution_date is " +
                  context.get("execution_date").strftime(time_format) +
                  " and the value of next_execution_date is " +
                  context.get("next_execution_date").strftime(time_format))
            print(
                "Current time is " + pendulum.now().strftime(time_format) +
                " which is lower than expected time " + desired_timestamp.strftime(time_format) + ". " +
                "Returning failure.")
            raise AirflowException("Forced failure.")
        else:
            print("The value of execution_date is " +
                  context.get("execution_date").strftime(time_format) +
                  " and the value of next_execution_date is " +
                  context.get("next_execution_date").strftime(time_format))
            print(
                "Current time is " + pendulum.now().strftime(time_format) +
                " which is higher than expected time " + desired_timestamp.strftime(time_format) +
                ". Returning success.")
            pass


def operator(waiting_time, *args, **kwargs):
    return WaitOperator(waiting_time=waiting_time, *args, **kwargs)
