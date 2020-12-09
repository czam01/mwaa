import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from jobsda import athena_query
from operators.slack_webhook_operator import SlackWebhookOperator
from operators.aws_athena_operator import AWSAthenaOperator

WORKFLOW_DEFAULT_ARGS = {
    'email': ['admin@clouding.la'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    dag_id='another_dag',
    description='Main DAG for da',
    schedule_interval='* * * * *',
    start_date=datetime(2020, 11, 1),
    catchup=False,
    default_args=WORKFLOW_DEFAULT_ARGS
)

slack_dag = SlackWebhookOperator(
    task_id='slack',
    http_conn_id='slack_connection',
    message='hello from slack',
    channel='#airflowchannel'
  )


run_query = AWSAthenaOperator(
    task_id='run_query',
    query='SELECT * FROM "mlpreparation"."ml_data_preparation" limit 10;',
    output_location='s3://airflow-demo-results/',
    database='mlpreparation'
)

load_athena = PythonOperator(
    task_id='athena_da',
    python_callable=athena_query.run,
    dag=dag
)

slack_dag >> run_query
