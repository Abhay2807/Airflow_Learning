from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

# --------------------
# Default arguments for the DAG
# --------------------
default_args = {
    'owner': 'airflow',                         # Owner of the DAG
    'retries': 0,                               # Number of retries if a task fails
    'retry_delay': timedelta(minutes=3)         # Delay between retries
}

# --------------------
# Function to return a name
# (This will be automatically pushed to XCom by Airflow)
# --------------------
def return_name():
    return "Abhay Singh"

# --------------------
# Function to pull name from XCom and print it along with age
# --------------------
def print_name(age, ti):
    name = ti.xcom_pull(task_ids='first_task')  # Pull name from XCom pushed by 'first_task'
    log = LoggingMixin().log
    log.info(f"Age is {age} years old, Name: {name}")

# --------------------
# Define the DAG
# --------------------
with DAG(
    dag_id='XCOM_DAG_Example_Two',                  # Unique identifier for the DAG
    description='Python Operator DAG with Better Code',  # Description for the DAG
    default_args=default_args,                      # Pass default arguments
    start_date=datetime(2025, 4, 25),                # Start date
    schedule='@daily',                              # Schedule to run daily
    catchup=False                                   # Do not backfill missed runs
) as dag:
    
    # Task 1: Returns the name (automatically pushed to XCom)
    task1 = PythonOperator(
        task_id='first_task',
        python_callable=return_name
    )

    # Task 2: Receives age as argument and pulls name from XCom
    task2 = PythonOperator(
        task_id='second_task',
        python_callable=print_name,
        op_kwargs={'age': 22}                       # Passing static argument 'age'
    )

    # Setting up task dependencies
    task1 >> task2
