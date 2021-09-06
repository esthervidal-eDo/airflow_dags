from airflow.models import Variable


def __airflow_var_to_env_var__(variable_name):
    split_name = variable_name.split(":")
    if len(split_name) == 1:
        return {variable_name: Variable.get(variable_name)}
    elif len(split_name) == 2:
        return {split_name[1]: Variable.get(split_name[0])}
    else:
        raise Exception("Airflow variable {} is not properly formatted!".format(variable_name))


def __get_docker_image__(docker_image_name):
    project = Variable.get("PROJECT")
    docker_image_relative_path = Variable.get(docker_image_name + "_DOCKER_IMAGE")
    if "eu.gcr.io" in docker_image_relative_path:
        raise Exception("Docker image {relative_path} should be written in relative path!".format(
            relative_path=docker_image_relative_path))
    docker_image_absolute_path = \
        "eu.gcr.io/{project}/{relative_path}".format(project=project, relative_path=docker_image_relative_path)
    return docker_image_absolute_path


def __update_environment_variables__(env_vars, secrets, airflow_vars):
    env_vars_updated = env_vars.copy()
    env_vars_updated.update({secret_location_env_var: Variable.get(secret_name)
                             for secret_name, secret_location_env_var in secrets.items()})
    for var in airflow_vars:
        env_vars_updated.update(__airflow_var_to_env_var__(var))
    return env_vars_updated


def __dict_to_colon__(d):
    return ["{}:{}".format(key, value) for key, value in d.items()]


def __colon_to_dict__(l):
    return dict([element.split(":") for element in l])
