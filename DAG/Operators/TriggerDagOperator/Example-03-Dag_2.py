from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
from pendulum import timezone

log = LoggingMixin().log
my_time_zone = timezone("Asia/Kolkata")

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Function to log received personal info
def get_person_info(**kwargs):
    info = kwargs['dag_run'].conf
    log.info(f"Name is : {info.get('name')}\n"
             f"Age is : {info.get('age')} years Old\n"
             f"Gender is {info.get('gender')} and city is {info.get('place')}")

# Data to pass to Dag_Number_Three
person_life_info = {
    "Company": "HDFC Bank",
    "WorkExp": 22,
    "Role": "Data Engineer",
    "Place": "Mumbai"
}

with DAG(
    dag_id='Dag_Number_Two',
    description='Second DAG to log personal info and trigger DAG 3',
    start_date=datetime(2025, 5, 3, tzinfo=my_time_zone),
    schedule=None,
    catchup=False,
    default_args=default_args
) as dag:

    start_task = EmptyOperator(task_id='start_task')

    get_person_info_task = PythonOperator(
        task_id='get_person_info_task',
        python_callable=get_person_info
    )

    # Trigger DAG 3 with person_life_info
    trigger_dag_three = TriggerDagRunOperator(
        task_id='trigger_dag_three',
        trigger_dag_id='Dag_Number_Three',
        conf=person_life_info,
        wait_for_completion=False,
        reset_dag_run=True
    )

    end_task = EmptyOperator(task_id='end_task')

    # Set task dependencies
    start_task >> get_person_info_task >> trigger_dag_three >> end_task
