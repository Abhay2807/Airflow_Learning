from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

# Default arguments for tasks
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

# Python function to read the conf passed by the parent DAG
def read_message(**kwargs):
    conf = kwargs['dag_run'].conf or {}           # Read DAG run config
    log = LoggingMixin().log                      # Use Airflow's logger
    log.info(f"Received message: {conf.get('msg')}")  # Log the message

# Define the Child DAG
with DAG(
    dag_id='Child_Dag_v1',
    description="This is Child Dag v1",
    start_date=datetime(2025, 5, 1),
    default_args=default_args,
    schedule=None,           # No automatic schedule — only triggered externally
    catchup=False
) as dag:

    # Task to read the message passed from the parent
    read_msg = PythonOperator(
        task_id='read_msg',
        python_callable=read_message
    )
