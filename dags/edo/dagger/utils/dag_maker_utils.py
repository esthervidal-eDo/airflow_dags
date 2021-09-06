from airflow import AirflowException
from airflow.models import Variable

import yaml
import requests

PIPELINER_SERVER = Variable.get("PIPELINER_SERVER")
PIPELINER_BASE_URL_TEMPLATE = 'http://{0}/schema-events'.format(PIPELINER_SERVER)
STRUCTURE_ETL_URL_TEMPLATE = 'etl/{0}/STRUCTURE'
SUBDAG_ETL_URL_TEMPLATE = 'etl/{0}/SUBDAG'


def extract_yaml_from_stream(stream):
    try:
        return yaml.safe_load(stream)
    except yaml.YAMLError as exc:
        raise AirflowException(exc)


def call_pipeliner_get_url(url, params=None):
    return call_get_url(url='{0}/{1}'.format(PIPELINER_BASE_URL_TEMPLATE, url), params=params)


def call_get_url(url, params=None):
    try:
        print(f"dag_maker_utils.call_get_url url: {url}")
        http_result = requests.get(url=url, params=params)
        http_result.raise_for_status()
        return http_result
    except requests.exceptions.RequestException as exc:
        raise AirflowException(exc)


def dict_from_path(path):
    with open(path, 'r') as stream:
        return extract_yaml_from_stream(stream)


def dict_from_url(url):
    http_result = call_pipeliner_get_url(url=url)
    return extract_yaml_from_stream(http_result.text)


def generate_subdag_dict_from_url(prefix):
    return dict_from_url(url=SUBDAG_ETL_URL_TEMPLATE.format(prefix))


def generate_dict_from_url(prefix, url=None, url_mask=None):
    if url:
        return dict_from_url(url=url)
    else:
        if not url_mask:
            raise AirflowException("'url_mask' not provided.")
        return dict_from_url(url=url_mask.format(prefix))


def generate_dict(prefix, path, url, url_mask):
    if path:
        return dict_from_path(path=path)
    else:
        return generate_dict_from_url(prefix, url, url_mask)


def generate_structure_dict(prefix, path, url):
    return generate_dict(prefix, path, url, STRUCTURE_ETL_URL_TEMPLATE)
