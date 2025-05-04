from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from pendulum import timezone

# Set up logging
log = LoggingMixin().log

# Set timezone to IST
my_time_zone = timezone("Asia/Kolkata")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Function to log personal information (uses 'params' passed at runtime)
def person_info(**kwargs):
    info = kwargs['params']
    log.info(
        f"My name is : {info.get('name')}\n"
        f"My age is {info.get('age')} years old and I am from {info.get('place')}"
    )

# Function to log job-related information
def job_info(**kwargs):
    info = kwargs['params']
    log.info(
        f"Company is {info.get('company')} with {info.get('workExp')} Months of Work Experience!!!"
    )

# Function to log favorite IPL team information
def team_info(**kwargs):
    info = kwargs['params']
    log.info(
        f"My favourite IPL team is {info.get('team')} which has already won {info.get('titles')} Titles!!!"
    )

# Define the DAG
with DAG(
    dag_id='Grouping_Tasks_DAG_v2',
    description='Tasks grouping DAG Example',
    start_date=datetime(2025, 5, 4, tzinfo=my_time_zone),
    schedule=None,                  # Manually triggered DAG
    catchup=False,                 # No backfilling
    default_args=default_args
) as dag:

    # Start and End dummy tasks
    start_task = EmptyOperator(
        task_id='start_task'
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    # Group 1: Person-related information
    with TaskGroup("person_group", tooltip="Task group containing Person Details") as person_group:
        person_task = PythonOperator(
            task_id='python_task',
            python_callable=person_info,
            params={
                "name": "Abhay Singh",
                "age": 22,
                "place": "Chandigarh"
            }
        )

        job_task = PythonOperator(
            task_id='job_task',
            python_callable=job_info,
            params={
                "company": "HDFC BANK",
                "workExp": 22
            }
        )

        # Job task runs after person task
        person_task >> job_task

    # Group 2: Team-related information
    with TaskGroup("team_group", tooltip="Task group containing Team Details") as team_group:
        team_task = PythonOperator(
            task_id='team_task',
            python_callable=team_info,
            params={
                "team": "MI",
                "titles": 5
            }
        )

    # Define task dependencies across groups
    start_task >> person_group >> team_group >> end_task
