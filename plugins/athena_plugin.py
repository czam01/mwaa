from airflow.plugins_manager import AirflowPlugin
from hooks.aws_athena_hook import AWSAthenaHook
from operators.aws_athena_operator import AWSAthenaOperator

class aws_athena_operator(SlackWebhookOperator):
  pass

class aws_athena_hook(SlackWebhookHook):
  pass

class slack_plugin(AirflowPlugin):
                    
    name = 'my_athena_plugin'       
    hooks = [aws_athena_hook]
    operators = [aws_athena_operator]