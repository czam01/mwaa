import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from jobsda import athena_query
from operators.slack_webhook_operator import SlackWebhookOperator


WORKFLOW_DEFAULT_ARGS = {
    'email': ['admin@clouding.la'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='demo_da_every',
    description='Main DAG for da',
    schedule_interval='* * * * *',
    start_date=datetime(2020, 11, 1),
    catchup=False,
    concurrency=3,
    default_args=WORKFLOW_DEFAULT_ARGS
)

slack_dag = SlackWebhookOperator(
    task_id='slack',
    http_conn_id='slack_connection',
    webhook_token='https://hooks.slack.com/services/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',
    message='hello from slack',
    channel='#airflowchannel'
  ).execute(context=None)


load_athena = PythonOperator(
    task_id='athena_da',
    python_callable=athena_query.run,
    dag=dag
)

slack_dag >> load_athena 
