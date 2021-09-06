from datetime import timedelta, datetime

from airflow import configuration, AirflowException
from airflow.models import DAG

from edo.dagger.utils.notifications_slack_utils import update_slack_notifications

DEFAULT_DESCRIPTION = ''
DEFAULT_SCHEDULE_INTERVAL = "@daily"
DEFAULT_END_DATE = None
DEFAULT_FULL_FILEPATH = None
DEFAULT_TEMPLATE_SEARCHPATH = None
DEFAULT_USER_DEFINED_MACROS = None
DEFAULT_USER_DEFINED_FILTERS = None
DEFAULT_CONCURRENCY = configuration.conf.getint('core', 'dag_concurrency')
DEFAULT_MAX_ACTIVE_RUNS = configuration.conf.getint('core', 'max_active_runs_per_dag')
DEFAULT_DAGRUN_TIMEOUT = None
DEFAULT_SLA_MISS_CALLBACK = None
DEFAULT_DEFAULT_VIEW = configuration.conf.get('webserver', 'dag_default_view').lower()
DEFAULT_ORIENTATION = configuration.conf.get('webserver', 'dag_orientation')
DEFAULT_CATCHUP = configuration.conf.getboolean('scheduler', 'catchup_by_default')
DEFAULT_ON_SUCCESS_CALLBACK = None
DEFAULT_ON_FAILURE_CALLBACK = None
DEFAULT_PARAMS = None
DEFAULT_DEFAULT_ARGS = None


def dict_to_dt_or_td(dictionary, what):
    """
    Converts a dictionary to a datetime or timedelta, unless the dictionary is empty, in which case returns None
    :param what: either datetime or timedelta
    :param dictionary:
    :return:
    """
    if what not in (datetime, timedelta):
        raise Exception("Wrong input")
    return dictionary and what(**dictionary) or None


def dictionary_to_timedelta(maybe_dictionary):
    if isinstance(maybe_dictionary, dict):
        return dict_to_dt_or_td(maybe_dictionary, timedelta)
    elif isinstance(maybe_dictionary, timedelta) or maybe_dictionary is None:
        return maybe_dictionary
    else:
        raise AirflowException("Something went wrong when trying to convert {} to a timedelta".format(maybe_dictionary))


def dictionary_to_datetime(maybe_dictionary):
    if isinstance(maybe_dictionary, dict):
        return dict_to_dt_or_td(maybe_dictionary, datetime)
    elif isinstance(maybe_dictionary, datetime) or maybe_dictionary is None:
        return maybe_dictionary
    else:
        raise AirflowException("Something went wrong when trying to convert {} to a datetime".format(maybe_dictionary))


def treat_default_args(default_args):
    if default_args is None:
        return {}
    else:
        if "retry_delay" in default_args.keys():
            retry_delay_dict = default_args.get("retry_delay")
            default_args.update(retry_delay=dictionary_to_timedelta(retry_delay_dict))
        if "slack_notification" in default_args.keys():
            default_args = update_slack_notifications(default_args)

    return default_args


def dag_instantiator(dag_name,
                     start_date,
                     schedule_interval,
                     description=DEFAULT_DESCRIPTION,
                     end_date=DEFAULT_END_DATE,
                     full_filepath=DEFAULT_FULL_FILEPATH,
                     template_searchpath=DEFAULT_TEMPLATE_SEARCHPATH,
                     user_defined_macros=DEFAULT_USER_DEFINED_MACROS,
                     user_defined_filters=DEFAULT_USER_DEFINED_FILTERS,
                     concurrency=DEFAULT_CONCURRENCY,
                     max_active_runs=DEFAULT_MAX_ACTIVE_RUNS,
                     dagrun_timeout=DEFAULT_DAGRUN_TIMEOUT,
                     sla_miss_callback=DEFAULT_SLA_MISS_CALLBACK,
                     default_view=DEFAULT_DEFAULT_VIEW,
                     orientation=DEFAULT_ORIENTATION,
                     catchup=DEFAULT_CATCHUP,
                     on_success_callback=DEFAULT_ON_SUCCESS_CALLBACK,
                     on_failure_callback=DEFAULT_ON_FAILURE_CALLBACK,
                     params=DEFAULT_PARAMS,
                     default_args=DEFAULT_DEFAULT_ARGS):
    parsed_schedule_interval = schedule_interval
    parsed_start_date = dictionary_to_datetime(start_date)
    parsed_end_date = dictionary_to_datetime(end_date)
    parsed_default_args = treat_default_args(default_args)
    return DAG(dag_id=dag_name,
               description=description,
               schedule_interval=parsed_schedule_interval,
               start_date=parsed_start_date,
               end_date=parsed_end_date,
               full_filepath=full_filepath,
               template_searchpath=template_searchpath,
               user_defined_macros=user_defined_macros,
               user_defined_filters=user_defined_filters,
               concurrency=concurrency,
               default_args=parsed_default_args,
               max_active_runs=max_active_runs,
               dagrun_timeout=dagrun_timeout,
               sla_miss_callback=sla_miss_callback,
               default_view=default_view,
               orientation=orientation,
               catchup=catchup,
               on_success_callback=on_success_callback,
               on_failure_callback=on_failure_callback,
               params=params)
