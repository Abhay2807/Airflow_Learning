from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

# --------------------
# Default arguments for the DAG
# --------------------
default_args = {
    'owner': 'airflow',                         # Owner name
    'retries': 0,                               # No retries
    'retry_delay': timedelta(minutes=3)         # Delay between retries (if any)
}

# --------------------
# Function to push data to XCom
# --------------------
def send_info_to_xcom(**kwargs):
    name = "Abhay Singh"
    ti = kwargs['ti']                           # Get TaskInstance
    ti.xcom_push(key='name', value=name)         # Push 'name' into XCom

# --------------------
# Function to pull data from XCom
# --------------------
def receive_info_from_xcom(**kwargs):
    ti = kwargs['ti']                           # Get TaskInstance
    pulled_name = ti.xcom_pull(
        task_ids='first_task',                   # From which task_id to pull
        key='name'                               # Key to pull
    )
    log = LoggingMixin().log
    log.info(f"Got Name from XCom: {pulled_name}")  # Log the pulled value
# --------------------

# Define the DAG
# --------------------
with DAG(
    dag_id='Simple_XCOM_DAG',                    # Unique identifier for DAG
    description='Python Operator DAG with Better Code',  # Description of the DAG
    default_args=default_args,                   # Pass default arguments
    start_date=datetime(2025, 4, 25),             # Start date
    schedule='@daily',                           # Run daily
    catchup=False                                # Do not run missed periods
) as dag:
    
    # Task 1: Push value to XCom
    task1 = PythonOperator(
        task_id='first_task',
        python_callable=send_info_to_xcom
    )

    # Task 2: Pull value from XCom
    task2 = PythonOperator(
        task_id='second_task',
        python_callable=receive_info_from_xcom
    )

    # Set task dependencies
    task1 >> task2
