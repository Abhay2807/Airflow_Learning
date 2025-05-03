from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
from pendulum import timezone

# Initialize logger
log = LoggingMixin().log

# Set timezone
my_time_zone = timezone("Asia/Kolkata")

# Default arguments for tasks
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Sample Python function to print templated values
def printValues(ds, **kwargs):
    details = kwargs['params']
    log.info(f"Execution date for the DAG is : {ds}")
    log.info(f"My name is {details.get('name')} and age is {details.get('Age')} years old")

# Info to be passed from Parent to Child DAG via TriggerDagRunOperator
info_to_send = {
    "Company": "HDFC Bank",
    "WorkExp": 22,
    "Role": "Data Engineer",
    "Place": "Mumbai"
}

# Define the Parent DAG
with DAG(
    dag_id='Templating_DAG_Parent',
    description='Templating Usecase in DAG',
    start_date=datetime(2025, 5, 3, tzinfo=my_time_zone),
    schedule='@daily',
    catchup=False,
    default_args=default_args
) as dag:

    # Start dummy task
    start_task = EmptyOperator(
        task_id='start_task'
    )

    # End dummy task
    end_task = EmptyOperator(
        task_id='end_task'
    )

    # Python task to print name and age (uses templated params)
    python_task = PythonOperator(
        task_id='python_task',
        python_callable=printValues,
        params={
            "name": "Abhay Singh",
            "Age": 22
        }
    )

    # Task to trigger child DAG and pass info using conf
    trigger_dag_task = TriggerDagRunOperator(
        task_id='trigger_dag_task',
        trigger_dag_id='Templating_DAG_Child',  # ID of child DAG
        wait_for_completion=False,
        conf=info_to_send
    )

    # Define execution flow
    start_task >> python_task >> trigger_dag_task >> end_task
