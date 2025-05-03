from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from pendulum import timezone

# Create a logger object
log = LoggingMixin().log

# Set timezone for the DAG
my_time_zone = timezone("Asia/Kolkata")

# Default arguments applied to all tasks unless overridden
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Python callable function used in PythonOperator
def printValues(**kwargs):
    details = kwargs['params']
    log = LoggingMixin().log
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

    # Dummy start task (used for DAG structure)
    start_task = EmptyOperator(
        task_id='start_task'
    )

    # Dummy end task
    end_task = EmptyOperator(
        task_id='end_task'
    )

    # BashOperator with Jinja templating to print execution date
    bash_task = BashOperator(
        task_id='bash_task',
        bash_command='echo "Execution date (Logical date for the DAG) is : {{ ds }}"'
    )

    # PythonOperator demonstrating use of params and templating
    python_task = PythonOperator(
        task_id='python_task',
        python_callable=printValues,
        params={
            "name": "Abhay Singh",
            "Age": 22
        }
    )

    # Define task dependencies
    start_task >> bash_task >> python_task >> end_task
