from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta

# Define default arguments that will be applied to all tasks in the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=7)
}

# Define the Python callable function for the task
def square_num(x: int):
    log = LoggingMixin().log
    y = x * x
    log.info(f"Square of {x} is {y}")

# Define the DAG
with DAG(
    dag_id="backfill_example_04",  # Unique identifier for the DAG
    description="Backfill does not depend on catchup, using CLI to run backfill",
    start_date=datetime(2025, 4, 25),  # Historical start date for backfill
    schedule='@daily',  # Daily scheduling
    default_args=default_args,  # Attach default args to the DAG
    catchup=False  # Prevent automatic backfilling by scheduler
) as dag:

    # Define the task using PythonOperator
    task1 = PythonOperator(
        task_id="square_number",          # Unique ID for the task
        python_callable=square_num,       # Function to execute
        op_kwargs={'x': 10}               # Keyword arguments passed to the function
    )

    # Set task order (in this case, only one task)
    task1
