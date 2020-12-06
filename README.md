#Managed Apache Airflow on AWS

# Deployment on MWAA

## Bucket for Plugins/DAGs/Requirments

*  You need to specify a bucket that must start with airlow in the name.
*  Specify different paths for Dags, Plugins and Requirements.

## Custom Configurations
*  Create a new VPC and select the type of deployment (public or private).
*  Select the environment class, to start I suggest mw1.small.
*  For monitoring in production environment it is better to enable form INFO logs for tasks, webserver, scheduler, worker and DAGs.
*  Create a custom role and give the role permissions over the S3 bucket. (Remember give permissions to the rone in the Bucket policy).

## DAG Structure
*  Store all the DAGs in the dags folder.
*  The DAGs should finish with .py extension.

## Lesson Learned 
*  Take into account that the latest version is 1.10.12 and you will have all the default packages that come with the version.
*  In the requirements file you need to specify additional packages in this way:  apache-airflow[slack, package2, package3].
*  If you need to use custom operators you need to call them from the dag file: from airflow.contrib.operators.slack_webhook_operator import SlackWebhookOperator





