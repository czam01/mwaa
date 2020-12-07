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
    catchup=False,
    default_args=WORKFLOW_DEFAULT_ARGS
)

slack_dag = SlackWebhookOperator(
    task_id='slack_message',
    http_conn_id='slack_connection',
    message='hello from slack',
    channel='#airflowchannel'
  )




load_athena = PythonOperator(
    task_id='athena_da',
    python_callable=athena_query.run,
    dag=dag
)

slack_dag >> load_athena
