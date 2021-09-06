import json
import traceback
import urllib

import requests
import urllib3
from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator
from airflow.hooks.base_hook import BaseHook
from jinja2 import Template

from edo.dagger.utils.dag_maker_utils import call_pipeliner_get_url

urllib3.disable_warnings()
import logging

"""
Default connections slack
"""
SLACK_CONN_ID = 'slack_conn'
USER_NAME = 'ds-data-airflow'
CHANNEL_SLACK = 'svc-dataengineer-etl-notifications'


def update_slack_notifications(args):
    """
    Method has called from dag_builder to send message in slack
    :param args:
    :return:
    """
    slack_notification = args.get("slack_notification")
    if slack_notification is not None:
        on_success_callback = slack_notification.get("on_success_callback")
        on_retry_callback = slack_notification.get("on_retry_callback")
        on_failure_callback = slack_notification.get("on_failure_callback")
        if on_failure_callback is not None and on_failure_callback:
            args.update(on_failure_callback=task_fail_slack_alert)
        if on_success_callback is not None and on_success_callback:
            args.update(on_success_callback=task_success_slack_alert)
        if on_retry_callback is not None and on_success_callback:
            args.update(on_retry_callback=task_retry_slack_alert)
    return args


def task_success_slack_alert(context):
    """
    Method define message and execute the success send message
    :param context:
    :return:
    """
    message = "Task succeded"
    blocks = prepare_message(context, "info", ":green_circle:", message)
    return execute_send_message_slack(context, blocks)


def task_retry_slack_alert(context):
    """
    Method define message and execute the success send message
    :param context:
    :return:
    """
    message = "Task retried"
    blocks = prepare_message(context, "info", ":large_orange_circle:", message)
    return execute_send_message_slack(context, blocks)


def task_fail_slack_alert(context):
    """
    Method define message and execute the failed send message
    :param context:
    :return:
    """
    message = "Task failed"
    blocks = prepare_message(context, "info", ":red_circle:", message)
    return execute_send_message_slack(context, blocks)


def logging_context(context):
    """
    For debugging, logging attributes context
    :param context:
    :return:
    """
    logging.info(context)
    logging.info(context.get('task_instance'))
    logging.info(context.get('task_instance').task['env_vars'])


def execute_send_message_slack_operator(context, blocks, message):
    slack_conn_id, slack_webhook_conn, user_name, channel_slack = get_conn_slack(context)
    alert = SlackWebhookOperator(
        task_id=context.get('task_instance').dag_id,
        http_conn_id=slack_conn_id,
        webhook_token=slack_webhook_conn.password,
        blocks=blocks,
        message=message,
        username=user_name,
        channel=channel_slack
    )
    logging.info(f"use operator:{blocks}")
    return alert.execute(context=context)


def execute_send_message_slack(context, blocks):
    """
    Method execute send message.
    :param blocks:
    :param context:
    :return:
    """
    try:
        slack_conn_id, slack_webhook_conn, user_name, channel_slack = get_conn_slack(context)
        # execute_send_message_slack_operator(context, blocks, "")
        url_slack = f"{slack_webhook_conn.host}/{slack_webhook_conn.password}"
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url_slack, data=json.dumps(blocks), headers=headers)
        if response.status_code != 200:
            raise ValueError(
                'Request to slack returned an error %s, the response is:\n%s'
                % (response.status_code, response.text)
            )
        logging.info("channel:{}".format(channel_slack))
    except Exception:
        logging.error(f"ERROR {traceback.format_exc()}")


def prepare_message(context, type_message, status, message):
    """
    Method builds message from template from using the pipeliner and jinja
    :param message:
    :param status:
    :param type_message:
    :param context:
    :return:
    """
    exec_date, params_url = build_params_url(context, status, type_message, message)
    url = "event/notifications/slack/airflow/{}".format(exec_date.strftime('%Y-%m-%d'))
    pipeliner_url = f"{url};{params_url}"
    http_result = call_pipeliner_get_url(pipeliner_url)
    if http_result.status_code == 200:
        slack_template = http_result.text.strip('\n')
        data = {'log_url': context.get('task_instance').log_url, 'message': message}
        template = Template(slack_template)
        output_from_parsed_template = template.render(data)
        logging.info(output_from_parsed_template)
        return json.loads(output_from_parsed_template)
    else:
        raise ValueError(
            'Request to slack returned an error %s, the response is:\n%s'
            % (http_result.status_code, http_result.text)
        )


def build_params_url(context, status, type_message, message):
    """
    Method builds params that need the Pipeliner request
    :param context:
    :param status:
    :param type_message:
    :param message:
    :return:
    """
    task_instance = context.get('task_instance')
    start_date = task_instance.start_date
    start_time = "%02d:%02d:%02d" % (start_date.hour, start_date.minute, start_date.second)
    exec_date = context.get('execution_date')
    params = {'status': status,
              'type': type_message,
              'dag': task_instance.dag_id,
              'task': task_instance.task_id,
              'start_date': start_date.strftime('%Y-%m-%d'),
              'start_time': start_time,
              'exec_date': exec_date,
              'message': message}
    params_url = urllib.parse.urlencode(params).replace('&', ';')
    return exec_date, params_url


def get_conn_slack(context):
    """
    Method builds the connection slack from environ params
    :return:
    """
    env_vars = context.get('task_instance').task['env_vars']
    logging.debug(f"env_vars: {env_vars}")
    channel_slack, slack_conn_id, user_name = set_attributes_connection(env_vars)
    logging.debug(f"slack_conn_id: {slack_conn_id} user_name: {user_name} channel_slack: {channel_slack}")
    slack_webhook_conn = BaseHook.get_connection(SLACK_CONN_ID)
    if slack_conn_id is not None:
        slack_webhook_conn = BaseHook.get_connection(slack_conn_id)
    return slack_conn_id, slack_webhook_conn, user_name, channel_slack


def set_attributes_connection(env_vars):
    slack_conn_id = env_vars.get('SLACK_CONN_ID', SLACK_CONN_ID)
    user_name = env_vars.get('USER_NAME', USER_NAME)
    channel_slack = env_vars.get('CHANNEL_SLACK', USER_NAME)
    if slack_conn_id is None:
        slack_conn_id = SLACK_CONN_ID
    return channel_slack, slack_conn_id, user_name
