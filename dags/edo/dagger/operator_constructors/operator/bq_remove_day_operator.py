from airflow.models import Variable
from airflow.operators.bash_operator import BashOperator


def operator(
        dag,
        task_id,
        destination_project_dataset_table,
        *args, **kwargs):
    project = Variable.get("PROJECT")
    bash_command = " ".join(
        ["bq", "rm", "--force", "--project_id", project, destination_project_dataset_table + "${{ ds_nodash }}"])
    return BashOperator(dag=dag, task_id=task_id, bash_command=bash_command)
