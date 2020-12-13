import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import SlackWebhookOperator
from jobsda import athena_query
from operators.slack_webhook_operator import SlackWebhookOperator



WORKFLOW_DEFAULT_ARGS = {
    'email': ['admin@clouding.la'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='example_dag_da',
    description='Main Dag for DA',
    schedule_interval='* * * * *',
    start_date=datetime(2017, 11, 1),
    catchup=False,
    concurrency=3,
    default_args=WORKFLOW_DEFAULT_ARGS
)

task1 = SlackWebhookOperator(
    task_id='task_1',
    http_conn_id='slack_connection',
    message='I am the tastk 1',
    channel='#airflowchannel'
  )

task11 = SlackWebhookOperator(
    task_id='task_1_1',
    http_conn_id='slack_connection',
    message='I am the tastk 1_!',
    channel='#airflowchannel'
  )

task12 = SlackWebhookOperator(
    task_id='task_1_2',
    http_conn_id='slack_connection',
    message='I am the tastk 1_2',
    channel='#airflowchannel'
  )

task13 = SlackWebhookOperator(
    task_id='task_1_3',
    http_conn_id='slack_connection',
    message='I am the tastk 1_3',
    channel='#airflowchannel'
  )

task14 = SlackWebhookOperator(
    task_id='task_1_4',
    http_conn_id='slack_connection',
    message='I am the tastk 1_4',
    channel='#airflowchannel'
  )
task15 = SlackWebhookOperator(
    task_id='task_1_5',
    http_conn_id='slack_connection',
    message='I am the tastk 1_5',
    channel='#airflowchannel'
  )
  task16 = SlackWebhookOperator(
    task_id='task_1_6',
    http_conn_id='slack_connection',
    message='I am the tastk 1_6',
    channel='#airflowchannel'
  )

task1 >> task12
task1 >> task13
task1 >> task14
task1 >> task15
task1 >> task16