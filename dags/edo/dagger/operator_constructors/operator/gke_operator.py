from datetime import timedelta

from airflow.contrib.kubernetes import secret
from airflow.contrib.operators.gcp_container_operator import GKEPodOperator
from airflow.models import Variable

DEFAULT_CMDS = None
DEFAULT_LABELS = None
DEFAULT_SECRETS = {}
DEFAULT_VOLUMES = None
DEFAULT_ENV_VARS = {}
DEFAULT_GET_LOGS = True
DEFAULT_ARGUMENTS = []
DEFAULT_NAMESPACE = "default"
DEFAULT_NODE_POOL = "n1-standard-4"
DEFAULT_RESOURCES = None
DEFAULT_XCOM_PUSH = False
DEFAULT_IN_CLUSTER = False
DEFAULT_ANNOTATIONS = None
DEFAULT_CONFIG_FILE = None
DEFAULT_HOSTNETWORK = False
DEFAULT_TOLERATIONS = None
DEFAULT_GCP_CONN_ID = 'google_cloud_default'
DEFAULT_AIRFLOW_VARS = []
DEFAULT_STEP_ARGUMENT = None
DEFAULT_VOLUME_MOUNTS = None
DEFAULT_NODE_SELECTORS = None
DEFAULT_CLUSTER_CONTEXT = None
DEFAULT_EXECUTION_TIMEOUT = {"hours": 3}
DEFAULT_IMAGE_PULL_SECRETS = None
DEFAULT_SERVICE_ACCOUNT_NAME = "default"
DEFAULT_IS_DELETE_OPERATOR_POD = False
DEFAULT_STARTUP_TIMEOUT_SECONDS = 120


# TODO add trigger rules
def operator(dag, task_id,
             docker_image_name,
             params=None,
             arguments=None,
             step_argument=DEFAULT_STEP_ARGUMENT,
             airflow_vars=None,
             secrets=None,
             env_vars=None,
             node_pool=DEFAULT_NODE_POOL,
             namespace=DEFAULT_NAMESPACE,
             execution_timeout=None,
             cmds=DEFAULT_CMDS,
             volume_mounts=DEFAULT_VOLUME_MOUNTS,
             volumes=DEFAULT_VOLUMES,
             in_cluster=DEFAULT_IN_CLUSTER,
             cluster_context=DEFAULT_CLUSTER_CONTEXT,
             labels=DEFAULT_LABELS,
             startup_timeout_seconds=DEFAULT_STARTUP_TIMEOUT_SECONDS,
             get_logs=DEFAULT_GET_LOGS,
             annotations=DEFAULT_ANNOTATIONS,
             resources=DEFAULT_RESOURCES,
             config_file=DEFAULT_CONFIG_FILE,
             xcom_push=DEFAULT_XCOM_PUSH,
             node_selectors=DEFAULT_NODE_SELECTORS,
             image_pull_secrets=DEFAULT_IMAGE_PULL_SECRETS,
             service_account_name=DEFAULT_SERVICE_ACCOUNT_NAME,
             is_delete_operator_pod=DEFAULT_IS_DELETE_OPERATOR_POD,
             hostnetwork=DEFAULT_HOSTNETWORK,
             tolerations=DEFAULT_TOLERATIONS,
             gcp_conn_id=DEFAULT_GCP_CONN_ID,
             *args, **kwargs):
    if execution_timeout is None:
        execution_timeout = DEFAULT_EXECUTION_TIMEOUT
    if env_vars is None:
        env_vars = DEFAULT_ENV_VARS
    if secrets is None:
        secrets = DEFAULT_SECRETS
    if airflow_vars is None:
        airflow_vars = DEFAULT_AIRFLOW_VARS
    if arguments is None:
        arguments = DEFAULT_ARGUMENTS
    env_vars_updated = __update_environment_variables__(env_vars, secrets, airflow_vars)
    project = Variable.get("PROJECT")
    image_pull_policy = Variable.get("DEFAULT_IMAGE_PULL_POLICY")
    docker_image = __get_docker_image__(docker_image_name)
    if step_argument:
        arguments = [step_argument] + arguments
    from edo.dagger.utils.wrappers.gke_operator_wrapper import GKEPodOperatorWrapper
    return GKEPodOperatorWrapper(
        dag=dag,
        task_id=task_id,
        arguments=arguments,
        params=params,
        image=docker_image,
        name=__make_pod_name__(docker_image_name, task_id),
        project_id=project,
        execution_timeout=timedelta(**execution_timeout),
        location=Variable.get("ZONE_KUBERNETES"),
        cluster_name=Variable.get("EXECUTOR_CLUSTER_NAME"),
        secrets=[__get_secret__(s) for s in secrets.keys()],
        node_pool=node_pool,
        namespace=namespace,
        env_vars=env_vars_updated,
        gcp_conn_id=gcp_conn_id,
        cmds=cmds,
        volume_mounts=volume_mounts,
        volumes=volumes,
        in_cluster=in_cluster,
        cluster_context=cluster_context,
        labels=labels,
        startup_timeout_seconds=startup_timeout_seconds,
        get_logs=get_logs,
        image_pull_policy=image_pull_policy,
        annotations=annotations,
        resources=resources,
        config_file=config_file,
        xcom_push=xcom_push,
        node_selectors=node_selectors,
        image_pull_secrets=image_pull_secrets,
        service_account_name=service_account_name,
        is_delete_operator_pod=is_delete_operator_pod,
        hostnetwork=hostnetwork,
        tolerations=tolerations
    )


def __make_pod_name__(docker_image_name, task_id):
    return "{}-{}".format(docker_image_name, task_id).replace("_", "-").lower()


def __airflow_var_to_env_var__(variable_name):
    split_name = variable_name.split(":")
    if len(split_name) == 1:
        return {variable_name: Variable.get(variable_name)}
    elif len(split_name) == 2:
        return {split_name[1]: Variable.get(split_name[0])}
    else:
        raise Exception("Airflow variable {} is not properly formatted!".format(variable_name))


def __update_environment_variables__(env_vars, secrets, airflow_vars):
    env_vars_updated = env_vars.copy()
    env_vars_updated.update({secret_location_env_var: Variable.get(secret_name)
                             for secret_name, secret_location_env_var in secrets.items()})
    for var in airflow_vars:
        env_vars_updated.update(__airflow_var_to_env_var__(var))
    return env_vars_updated


def __get_secret__(secret_name):
    return secret.Secret(
        deploy_type='volume',
        secret=secret_name,
        deploy_target="/".join(Variable.get(secret_name).split("/")[:-1]),
        key=Variable.get(secret_name).split("/")[-1])


def __get_docker_image__(docker_image_name):
    project = Variable.get("PROJECT")
    docker_image_relative_path = Variable.get(docker_image_name + "_DOCKER_IMAGE")
    if "eu.gcr.io" in docker_image_relative_path:
        raise Exception("Docker image {relative_path} should be written in relative path!".format(
            relative_path=docker_image_relative_path))
    docker_image_absolute_path = \
        "eu.gcr.io/{project}/{relative_path}".format(project=project, relative_path=docker_image_relative_path)
    return docker_image_absolute_path


def __get_node_affinity__(node_pool_name):
    return {
        'nodeAffinity': {
            'requiredDuringSchedulingIgnoredDuringExecution': {
                'nodeSelectorTerms': [{
                    'matchExpressions': [{
                        'key': 'cloud.google.com/gke-nodepool',
                        'operator': 'In',
                        'values': [
                            node_pool_name
                        ]
                    }]
                }]
            }
        }
    }
