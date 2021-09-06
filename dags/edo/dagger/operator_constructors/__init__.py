from importlib import import_module

from airflow import AirflowException


def __last_name__(operator):
    return operator.split("_")[-1]


def __get_kind__(operator_name):
    last_name = __last_name__(operator_name)
    if last_name not in ("operator", "sensor"):
        raise AirflowException("{} doesn't match the naming conventions!".format(last_name))
    return last_name


def import_operator(operator):
    absolute_import = "{base}.{kind}.{operator}".format(
        base="edo.dagger.operator_constructors",
        kind=__get_kind__(operator),
        operator=operator)
    try:
        return import_module(absolute_import).operator
    except ModuleNotFoundError as e:
        raise AirflowException("Unknown operator kind {}!".format(operator), e)
