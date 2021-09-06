from airflow import AirflowException

from edo.dagger.operator_constructors.operator.utils import __airflow_var_to_env_var__


def operator(
        image,
        step_argument=None,
        airflow_vars=None,
        secrets=None,
        command=None,
        environment=None,
        volumes=None,
        *args,
        **kwargs):
    from edo.dagger.utils.wrappers.docker_operator_wrapper import DockerOperatorWrapper
    if environment is None:
        environment = []
    if volumes is None:
        volumes = []
    __locate_all_volumes__(volumes)
    if airflow_vars is None:
        airflow_vars = []
    if secrets is None:
        secrets = {}
    for var in airflow_vars:
        environment.update(__airflow_var_to_env_var__(var))
    for secret_name, secret in secrets.items():
        secret_location = secret.get("image_location", "/run/secrets/" + secret_name)
        environment[secret.get("env_var")] = secret_location
        volumes.append("{}:{}".format(secret["host_location"], secret_location))

    if step_argument:
        command = step_argument + " " + command

    return DockerOperatorWrapper(
        image=image,
        environment=environment,
        volumes=volumes,
        command=command,
        *args, **kwargs
    )


def __locate_volume__(volume):
    if ":" not in volume:
        raise AirflowException("Volume {} is not properly formatted!".format(volume))
    return volume


def __locate_all_volumes__(volumes):
    return [__locate_volume__(v) for v in volumes]
