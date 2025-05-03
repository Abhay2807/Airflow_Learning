from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
from airflow.utils.trigger_rule import TriggerRule
from pendulum import timezone

log=LoggingMixin().log
my_time_zone=timezone("Asia/Kolkata")

default_args={
    'owner':'airflow',
    'retries':0,
    'retry_delay':timedelta(minutes=5)
}

def get_person_info(ds,**kwargs):

    info=kwargs['dag_run'].conf
    log.info(f"Company is : {info.get('Company')}\n"
             f"Work Experience is : {info.get("WorkExp")} months\n"
             f"Job Role is {info.get("Role")} and office is in {info.get("Place")}\n"
             )
    log.info(f"Execution date for this Child DAG is {ds}")
    

person_life_info={
    "Company":"HDFC Bank",
    "WorkExp":22,
    "Role":"Data Engineer",
    "Place":"Mumbai"
}

with DAG(
    dag_id='Templating_DAG_Child',
    description='Templating Dag Usecase',
    start_date=datetime(2025,5,3,tzinfo=my_time_zone),
    schedule=None,
    catchup=False,
    default_args=default_args
) as dag:
    

    start_task=EmptyOperator(
        task_id='start_task'
    )

    end_task=EmptyOperator(
        task_id='end_task'
    )

    get_person_info_task=PythonOperator(
        task_id='get_person_info_task',
        python_callable=get_person_info
    )

    start_task>>get_person_info_task>>end_task
