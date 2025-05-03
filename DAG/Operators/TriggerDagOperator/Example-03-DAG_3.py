from airflow import DAG
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

# Function to log company and job info
def get_person_life_info(**kwargs):
    info = kwargs['dag_run'].conf
    log.info(f"Company is : {info.get('Company')}\n"
             f"Work Experience is : {info.get('WorkExp')} months\n"
             f"Job Role is {info.get('Role')} and office is in {info.get('Place')}")

with DAG(
    dag_id='Dag_Number_Three',
    description='Third DAG to log job info',
    start_date=datetime(2025, 5, 3, tzinfo=my_time_zone),
    schedule=None,
    catchup=False,
    default_args=default_args
) as dag:

    start_task = EmptyOperator(task_id='start_task')

    get_person_life_info_task = PythonOperator(
        task_id='get_person_life_info_task',
        python_callable=get_person_life_info
    )

    end_task = EmptyOperator(task_id='end_task')

    # Set task dependencies
    start_task >> get_person_life_info_task >> end_task
