from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

# Python function to read the message passed from the parent DAG
def read_message(**kwargs):
    conf = kwargs['dag_run'].conf                       # Retrieve config
    log = LoggingMixin().log                            # Access logger
    log.info(f"Received message: {conf.get('msg')}")    # Log the message

# Define Child DAG 1
with DAG(
    dag_id='Child_Dag_v1',                              # Unique ID for child DAG
    description="This is Child Dag One",                # Description
    start_date=datetime(2025, 5, 1),                    # Start date
    default_args=default_args,                          # Default arguments
    schedule=None,                                      # Triggered only externally
    catchup=False
) as dag:

    # PythonOperator to execute read_message function
    read_msg = PythonOperator(
        task_id='read_msg',
        python_callable=read_message
    )
