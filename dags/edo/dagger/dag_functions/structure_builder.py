from airflow import AirflowException

from edo.dagger.dag_functions.dag_builder import dag_instantiator
from edo.dagger.dag_functions.task_builder import task_instantiator


def get_links(tasks_dict):
    return {key: value.get("upstream_tasks") for key, value in tasks_dict.items()}


def get_node(dag, params, task_id, task_options, operators_dict):
    operator_name = task_options.get("operator")
    other_task_options = {key: value for key, value in task_options.items() if key != "operator"}
    return task_instantiator(
        operator_name, operators_dict,
        dag=dag,
        task_id=task_id,
        params=params,
        other_task_options=other_task_options)


def get_nodes(dag, tasks_dict, operators_dict, params=None):
    if params is None:
        params = {}
    return dict(
        (task_id, get_node(dag, params, task_id, task_options, operators_dict))
        for task_id, task_options in tasks_dict.items())


def link_task_names(tasks_dict, task_name, upstream_task_name):
    task = tasks_dict.get(task_name)
    upstream_task = tasks_dict.get(upstream_task_name)
    if task is None:
        raise AirflowException("Unknown task {task_name}".format(task_name=task_name))
    elif upstream_task is None:
        raise AirflowException(
            "{task_name}: impossible to link to {upstream_task_name} "
            "(unknown task)".format(task_name=task_name, upstream_task_name=upstream_task_name))
    task.set_upstream(upstream_task)


def get_linked_nodes(nodes, links):
    all_tasks = []
    for task_name, upstream_task_names in links.items():
        if upstream_task_names is None:
            continue
        elif isinstance(upstream_task_names, str):
            link_task_names(nodes, task_name, upstream_task_names)
        elif all(isinstance(item, str) for item in upstream_task_names):
            for item in upstream_task_names:
                link_task_names(nodes, task_name, item)
        else:
            raise AirflowException(
                "upstream_tasks option for {task_name}"
                " is neither a string nor a list of strings".format(task_name=task_name))
        task = nodes.get(task_name)
        all_tasks.append(task)
    return all_tasks


def make_structure(dag_name, params, variables, structure):
    tasks_dict = structure.get("tasks")
    operators_dict = structure.get("operators")
    print(f"make_structure.params: {params}")
    dag = dag_instantiator(dag_name, **variables)
    nodes = get_nodes(dag=dag, params=params, tasks_dict=tasks_dict, operators_dict=operators_dict)
    links = get_links(tasks_dict=tasks_dict)
    ops = get_linked_nodes(nodes=nodes, links=links)
    return dag, ops
