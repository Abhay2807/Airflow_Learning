from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from pendulum import timezone

# Initialize logger
log = LoggingMixin().log

# Set DAG timezone to Asia/Kolkata
my_time_zone = timezone("Asia/Kolkata")

# Default arguments for the DAG tasks
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Python function used in PythonOperator
# 'ds' is automatically passed by Airflow as execution date
# 'params' dictionary is passed via the operator
def printValues(ds, **kwargs):
    details = kwargs['params']
    log = LoggingMixin().log
    log.info(f"Execution date for the DAG is : {ds}")
    log.info(f"My name is {details.get('name')} and age is {details.get('Age')} years old")

# Define the DAG
with DAG(
    dag_id='Templating_DAG',
    description='Templating Usecase in DAG',
    start_date=datetime(2025, 5, 3, tzinfo=my_time_zone),
    schedule='@daily',
    catchup=False,
    default_args=default_args
) as dag:

    # Dummy task to represent DAG start
    start_task = EmptyOperator(
        task_id='start_task'
    )

    # Dummy task to represent DAG end
    end_task = EmptyOperator(
        task_id='end_task'
    )

    # Task to demonstrate templating with execution date and params
    python_task = PythonOperator(
        task_id='python_task',
        python_callable=printValues,
        params={
            "name": "Abhay Singh",
            "Age": 22
        }
    )

    # Set task execution order: start -> python_task -> end
    start_task >> python_task >> end_task
