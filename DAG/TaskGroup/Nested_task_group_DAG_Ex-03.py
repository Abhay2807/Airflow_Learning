from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator               # Used as dummy start/end markers
from airflow.utils.log.logging_mixin import LoggingMixin        # For logging inside Python callables
from airflow.operators.python import PythonOperator             # For Python-based tasks
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup                  # Used to create TaskGroups
from pendulum import timezone                                   # Handles timezone-aware datetime

# Create a logger instance
log = LoggingMixin().log

# Set timezone for DAG execution
my_time_zone = timezone("Asia/Kolkata")

# Default arguments applied to all tasks
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# --------- Python callables for different task groups ---------

def person_info(**kwargs):
    info = kwargs['params']
    log.info(f"My name is : {info.get('name')}\n"
             f"My age is {info.get('age')} years old and I am from {info.get('place')}")

def job_info(**kwargs):
    info = kwargs['params']
    log.info(f"Company is {info.get('company')} with {info.get('workExp')} Months of Work Experience!!!")

def team_info(**kwargs):
    info = kwargs['params']
    log.info(f"My favourite IPL team is {info.get('team')} which has already won {info.get('titles')} Titles!!!")

def college_info(**kwargs):
    info = kwargs['params']
    log.info(f"My college is {info.get('college')} and I secured an CGPA of {info.get('cgpa')}")

def player_info(**kwargs):
    info = kwargs['params']
    log.info(f"My favourite player is {info.get('player')} who plays for {info.get('team')}"
             f"He is known as {info.get('record')} with the major shot as {info.get('shot')}")


# ----------------- DAG Definition -----------------

with DAG(
    dag_id='Nested_Task_Groups_DAG',
    description='Nested Tasks grouping DAG Example',
    start_date=datetime(2025, 5, 5, tzinfo=my_time_zone),
    schedule=None,
    catchup=False,
    default_args=default_args
) as dag:

    # DAG entry and exit points
    start_task = EmptyOperator(task_id='start_task')
    end_task   = EmptyOperator(task_id='end_task')

    # -------------------- Outer TaskGroup: person_group --------------------
    with TaskGroup("person_group", tooltip="Task group containing Person Details") as person_group:

        start_person_group = EmptyOperator(task_id='start_pg')

        # First task inside person_group: logs personal info
        person_task = PythonOperator(
            task_id='python_task',
            python_callable=person_info,
            params={
                "name": "Abhay Singh",
                "age": 22,
                "place": "Chandigarh"
            }
        )

        # --------- Nested TaskGroup: inner_task_group ---------
        with TaskGroup("inner_task_group", tooltip="Inner Task Group") as inner_task_group:
            
            # Logs college info
            college_task = PythonOperator(
                task_id='college_task',
                python_callable=college_info,
                params={
                    "college": "NIT Hamirpur",
                    "cgpa": 9.56
                }
            )

            # Logs favorite player info
            player_task = PythonOperator(
                task_id='player_task',
                python_callable=player_info,
                params={
                    "team": "MI",
                    "player": "Rohit Sharma",
                    "record": "Biggest Six Hitter",
                    "shot": "Pull Shot"
                }
            )

            # Player info runs before college info
            player_task >> college_task
        # --------- End of inner_task_group ---------

        # Logs job information after nested group
        job_task = PythonOperator(
            task_id='job_task',
            python_callable=job_info,
            params={
                "company": "HDFC BANK",
                "workExp": 22
            }
        )

        end_person_group = EmptyOperator(task_id='end_pg')

        # Define the sequence of execution within person_group
        start_person_group >> person_task >> inner_task_group
        inner_task_group >> job_task >> end_person_group

    # -------------------- Another TaskGroup: team_group --------------------
    with TaskGroup("team_group", tooltip="Task group containing Team Details") as team_group:

        team_task = PythonOperator(
            task_id='team_task',
            python_callable=team_info,
            params={
                "team": "MI",
                "titles": 5
            }
        )

    # Final DAG execution flow
    start_task >> person_group >> team_group >> end_task
