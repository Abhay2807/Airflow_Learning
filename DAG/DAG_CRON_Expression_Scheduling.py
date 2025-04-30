# DAG to run every Monday at 6:30 PM IST
# Today: 30 Apr 2025
# Start Date: 01 Apr 2025 (a Tuesday)
# Catchup: True -> this will trigger past runs (i.e., 7 Apr, 14 Apr, 21 Apr, 28 Apr)
# Cron schedule: '30 18 * * 1' -> every Monday at 18:30 IST

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
from pendulum import timezone

# Set local timezone to IST
local_tz = timezone("Asia/Kolkata")

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=7)
}

# Define the Python function to be called by the task
def square_num(x: int):
    log = LoggingMixin().log
    y = x * x
    log.info(f"Square of {x} is {y}")

# Define the DAG
with DAG(
    dag_id="dag_schedule_cron_example_01_IST",                # Unique identifier for the DAG
    description="Scheduling DAG using CRON Expression",        # Description for UI
    start_date=datetime(2025, 4, 1, tzinfo=local_tz),          # Start date with IST timezone
    schedule='30 18 * * 1',                                    # Cron: Every Monday at 6:30 PM IST
    default_args=default_args,                                 # Default arguments
    catchup=True                                               # Enable backfill for missed runs
) as dag:

    # Define the task using PythonOperator
    task1 = PythonOperator(
        task_id="square_number",                               # Task ID
        python_callable=square_num,                            # Function to call
        op_kwargs={'x': 10}                                     # Arguments for the function
    )

    task1  # No dependencies, single task
