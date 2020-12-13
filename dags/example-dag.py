import os
from datetime import datetime, timedelta
from airflow import DAG
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
    channel='#airflowchannel',
    dag=dag
  )

task11 = SlackWebhookOperator(
    task_id='task_1_1',
    http_conn_id='slack_connection',
    message='I am the tastk 1_!',
    channel='#airflowchannel',
    dag=dag
  )

task12 = SlackWebhookOperator(
    task_id='task_1_2',
    http_conn_id='slack_connection',
    message='I am the tastk 1_2',
    channel='#airflowchannel',
    dag=dag
  )

task13 = SlackWebhookOperator(
    task_id='task_1_3',
    http_conn_id='slack_connection',
    message='I am the tastk 1_3',
    channel='#airflowchannel',
    dag=dag
  )

task14 = SlackWebhookOperator(
    task_id='task_1_4',
    http_conn_id='slack_connection',
    message='I am the tastk 1_4',
    channel='#airflowchannel',
    dag=dag
  )
task15 = SlackWebhookOperator(
    task_id='task_1_5',
    http_conn_id='slack_connection',
    message='I am the tastk 1_5',
    channel='#airflowchannel',
    dag=dag
  )
task16 = SlackWebhookOperator(
    task_id='task_1_6',
    http_conn_id='slack_connection',
    message='I am the tastk 1_6',
    channel='#airflowchannel',
    dag=dag
 )
task161 = SlackWebhookOperator(
    task_id='task_1_6_1',
    http_conn_id='slack_connection',
    message='I am the tastk 1_6_1',
    channel='#airflowchannel',
    dag=dag
 )
task131 = SlackWebhookOperator(
    task_id='task_1_3_1',
    http_conn_id='slack_connection',
    message='I am the tastk 1_3_1',
    channel='#airflowchannel',
    dag=dag
 )

task131161 = SlackWebhookOperator(
    task_id='task_1_3_1_1_6_1',
    http_conn_id='slack_connection',
    message='I am the tastk 1_3_1_1_6_1',
    channel='#airflowchannel',
    dag=dag
 )

task2 = SlackWebhookOperator(
    task_id='task_2',
    http_conn_id='slack_connection',
    message='I am the tastk 2',
    channel='#airflowchannel',
    dag=dag
  )

task21 = SlackWebhookOperator(
    task_id='task_2_1',
    http_conn_id='slack_connection',
    message='I am the tastk 2_1',
    channel='#airflowchannel',
    dag=dag
  )
task3 = SlackWebhookOperator(
    task_id='task_3',
    http_conn_id='slack_connection',
    message='I am the tastk 3',
    channel='#airflowchannel',
    dag=dag
  )
task4 = SlackWebhookOperator(
    task_id='task_4',
    http_conn_id='slack_connection',
    message='I am the tastk 4',
    channel='#airflowchannel',
    dag=dag
  )
task234 = SlackWebhookOperator(
    task_id='task_2_3_4',
    http_conn_id='slack_connection',
    message='I am the tastk 2_3_4',
    channel='#airflowchannel',
    dag=dag
  )


task1 >> task11
task1 >> task12
task1 >> task13
task1 >> task14
task1 >> task15
task1 >> task16
task16 >> task161
task13 >> task131
task131 >> task131161
task161 >> task131161
task2 >> task234
task3 >> task234
task4 >> task234
task234 >> task131161