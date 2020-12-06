import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from jobsda import athena_query



WORKFLOW_DEFAULT_ARGS = {
    'email': ['admin@clouding.la'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    dag_id='main_datapipeline_da',
    description='Main Dag for DA',
    schedule_interval='0 4 * * *',
    start_date=datetime(2017, 11, 1),
    catchup=False,
    concurrency=3,
    default_args=WORKFLOW_DEFAULT_ARGS
)

task1 = PythonOperator(
    task_id='extract_task1_all',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

accounts_mongo = PythonOperator(
    task_id='extract_mongo_accounts',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

profiles_mongo = PythonOperator(
    task_id='extract_mongo_profiles',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

user_contacts_mongo = PythonOperator(
    task_id='extract_mongo_user_contacts',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

verified_contacts_mongo = PythonOperator(
    task_id='extract_mongo_verified_contacts',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

task_glue = PythonOperator(
    task_id='glue_process_task',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

referrals_glue = PythonOperator(
    task_id='glue_process_referrals',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

prizes_glue = PythonOperator(
    task_id='glue_process_prizes',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

cashouts_glue = PythonOperator(
    task_id='glue_process_cashouts',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

cashins_glue = PythonOperator(
    task_id='glue_process_cashins',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

phone_contacts_glue = PythonOperator(
    task_id='glue_process_phone_contacts',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

customer_card_receptions_glue = PythonOperator(
    task_id='glue_process_customer_card_receptions',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

email_verifications_glue = PythonOperator(
    task_id='glue_process_email_verifications',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

customer_card_purchases_glue = PythonOperator(
    task_id='glue_process_customer_card_purchases',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

identity_verifications_glue = PythonOperator(
    task_id='glue_process_identity_verifications',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

tef_verifications_glue = PythonOperator(
    task_id='glue_process_tef_verifications',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

task1 >> task_glue
task1 >> referrals_glue
task1 >> prizes_glue
task1 >> cashouts_glue
task1 >> cashins_glue
task1 >> phone_contacts_glue
task1 >> customer_card_receptions_glue
task1 >> email_verifications_glue
task1 >> customer_card_purchases_glue
task1 >> identity_verifications_glue
task1 >> tef_verifications_glue

accounts_glue = PythonOperator(
    task_id='glue_process_accounts',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

profiles_glue = PythonOperator(
    task_id='glue_process_profiles',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

verified_contacts_glue = PythonOperator(
    task_id='glue_process_verified_contacts',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

user_contacts_glue = PythonOperator(
    task_id='glue_process_user_contacts',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

accounts_mongo >> accounts_glue
profiles_mongo >> profiles_glue
verified_contacts_mongo >> verified_contacts_glue
user_contacts_mongo >> user_contacts_glue

user_agg_daily_glue = PythonOperator(
    task_id='glue_calc_daily_user_agg',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

user_detail_glue = PythonOperator(
    task_id='glue_process_user_detail',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

task_glue >> user_agg_daily_glue
prizes_glue >> user_agg_daily_glue
user_agg_daily_glue >> user_detail_glue
accounts_glue >> user_detail_glue
profiles_glue >> user_detail_glue
verified_contacts_glue >> user_detail_glue
user_contacts_glue >> user_detail_glue
task_glue >> user_detail_glue
cashouts_glue >> user_detail_glue
referrals_glue >> user_detail_glue


clean_phone_contacts = PythonOperator(
    task_id='clean_phone_contacts',
    python_callable=athena_query.run,
    dag=dag
)


load_drop_phone_contacts_query = PythonOperator(
    task_id='load_drop_phone_contacts_query',
    python_callable=athena_query.run,
    dag=dag
)

load_create_phone_contacts_query = PythonOperator(
    task_id='load_create_phone_contacts_query',
    python_callable=athena_query.run,
    dag=dag
)

phone_contacts_glue >> clean_phone_contacts
clean_phone_contacts >> load_drop_phone_contacts_query
load_drop_phone_contacts_query >> load_create_phone_contacts_query

deliver_accounts_athena = PythonOperator(
    task_id='athena_deliver_accounts',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

accounts_glue >> deliver_accounts_athena

deliver_user_detail_athena = PythonOperator(
    task_id='athena_deliver_user_detail',
    python_callable=athena_query.run,
    provide_context=True,
    dag=dag
)

user_detail_glue >> deliver_user_detail_athena

load_customer_registrations = PythonOperator(
    task_id='load_customer_registrations',
    python_callable=athena_query.run,
    provide_context=False,
    dag=dag
)

deliver_accounts_athena >> load_customer_registrations


calc_contacts_score = PythonOperator(
    task_id='calc_contacts_score',
    python_callable=athena_query.run,
    dag=dag
)

table = "contacts_score"
query_file = 'create_table_contacts_score.sql'
load_create_contacts_score_query = PythonOperator(
    task_id='load_create_contacts_score_query',
    python_callable=athena_query.run,
    dag=dag
)

table = "contacts_score_detail"
query_file = 'create_table_contacts_score_detail.sql'
load_create_contacts_score_detail_query = PythonOperator(
    task_id='load_create_contacts_score_detail_query',
    python_callable=athena_query.run,
    dag=dag
)

load_create_phone_contacts_query >> calc_contacts_score
deliver_user_detail_athena >> calc_contacts_score
calc_contacts_score >> load_create_contacts_score_query
calc_contacts_score >> load_create_contacts_score_detail_query
