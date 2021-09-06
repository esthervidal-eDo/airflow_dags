from airflow import AirflowException

from edo.dagger.operator_constructors import import_operator
from edo.dagger.utils.utils import dict_merge


def task_instantiator(operator, operators_dict, dag, task_id, params=None, other_task_options=None):
    if other_task_options is None:
        other_task_options = {}
    if params is None:
        params = {}
    operator_options = operators_dict.get(operator)
    if operator_options is None:
        raise AirflowException("{task_id}: unknown operator {operator}".format(task_id=task_id, operator=operator))
    operator_kind = operator_options.get("operator_kind", operator)
    if not operator_options.get("prevent_overrides", False):
        operator_options = dict_merge(operator_options, other_task_options)
    operator_function = import_operator(operator_kind)
    return operator_function(dag=dag, task_id=task_id, params=params, **operator_options)
