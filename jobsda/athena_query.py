import os
import sys
import boto3

s3 = boto3.client('s3')

def run():
    client = boto3.client('athena')
    response = client.start_query_execution(
        QueryString= '''
CREATE EXTERNAL TABLE manual_validations(
  ticket_id string,
  document_number string,
  result string,
  created_at string,
  updated_at string)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY ','
STORED AS INPUTFORMAT
  'org.apache.hadoop.mapred.TextInputFormat'
OUTPUTFORMAT
  'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION
  's3://airflow-demo-results/'
TBLPROPERTIES (
  'classification'='csv',
  'columnsOrdered'='true',
  'compressionType'='none',
  'delimiter'=',',
  'skip.header.line.count'='1',
  'typeOfData'='file') ''',
        QueryExecutionContext={
            'Database': 'mlpreparation'
            },
        ResultConfiguration={
            'OutputLocation': 's3://airflow-demo-results/',
            }
        )
    print('Execution ID: ' + response['QueryExecutionId'])
    return response 