from datetime import datetime

from airflow import AirflowException
from airflow.models import Variable, DAG
from airflow.operators.subdag_operator import SubDagOperator

from edo.dagger.dag_functions.structure_builder import make_structure
from edo.dagger.utils.utils import find_regex
from edo.dagger.utils.dag_maker_utils import dict_from_path, generate_structure_dict

PROJECT = Variable.get("PROJECT")

def naming_convention(*conditions):
    return "_".join(map(str,conditions)).replace('-', '_').replace('.', '_').upper()

class DagMaker:
    def __init__(self, prefix, path=None, url=None, name_params=None):
        if name_params is None:
            name_params = []
        self.prefix = prefix
        self.name_params = name_params
        self.dct = generate_structure_dict(prefix, path, url)
        print(f"DagMaker.structure_dag: {self.dct}")

    def naming_convention(self, params):
        conditions = [params.get(param_value) for param_value in self.name_params]
        return "_".join([self.prefix] + conditions).replace('-', '_').replace('.', '_').upper()

    def check_params(self, params):
        print(f"check_params.PARAMS:{params}")
        given_params = params.keys()
        required_params = find_regex(document=self.dct, regex="{{ ?params.([0-z_]+) ?}}")
        params_required_not_given = [param for param in required_params if param not in given_params]
        params_given_not_required = [param for param in given_params if param not in required_params]
        if params_required_not_given:
            raise AirflowException("Required params missing: " +
                                   ", ".join(params_required_not_given))
        if params_given_not_required:
            raise AirflowException("Unexpected params: " +
                                   ", ".join(params_given_not_required) +
                                   ". Expected: " +
                                   ", ".join(required_params))

    def make_dag(self, *args, **params):
        if args:
            raise AirflowException(
                "Make_dag() requires keyword arguments, please include keys for the following arguments: " +
                ", ".join(
                    args))
        print(f"make_dag.args:{args}")
        print(f"make_dag.PARAMS:{params}")
        self.check_params(params)
        dag_name = self.naming_convention(params)
        dct_copy = self.dct.copy()
        dag, ops = make_structure(
            dag_name=dag_name,
            params=params,
            variables=dct_copy.get("variables"),
            structure=dct_copy.get("structure"))
        return dag, ops

    def make_subdag(self, overdag, *args, **params):
        from airflow.executors.celery_executor import CeleryExecutor
        if args:
            raise AirflowException(
                "make_subdag() requires keyword arguments, please include keys for the following arguments: " +
                ", ".join(args))
        dct_copy = self.dct.copy()
        dct_copy["variables"]["schedule_interval"] = overdag.schedule_interval
        dct_copy["variables"]["start_date"] = overdag.start_date
        task_id = self.naming_convention(params)
        subdag_name = "{0}.{1}".format(overdag.dag_id, task_id)
        subdag, _ = make_structure(
            dag_name=subdag_name,
            params=params,
            variables=dct_copy.get("variables"),
            structure=dct_copy.get("structure"))
        return SubDagOperator(subdag=subdag, dag=overdag, task_id=task_id, executor=CeleryExecutor())

    def make_overdag(self, overall_schedule_interval, cases_path):
        overdag = DAG(
            dag_id=naming_convention(self.prefix, "OVERDAG"),
            schedule_interval=overall_schedule_interval,
            start_date=datetime(2016, 1, 1),
            catchup=False)
        cases = dict_from_path(path=cases_path)
        subdags = [self.make_subdag(overdag=overdag, **params) for params in cases]
        return overdag, subdags
