from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from pendulum import timezone

# Set local timezone for scheduling
local_tz = timezone("Asia/Kolkata")

# Default arguments applied to all tasks
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=7)
}

# Function to decide branch based on whether the input number is even or odd
def select_branch(x: int):
    if x % 2 == 0:
        return 'even_branch'  # Route to even_branch if x is even
    else:
        return 'odd_branch'   # Route to odd_branch if x is odd

# Task function for the even branch
def even_num():
    log = LoggingMixin().log
    log.info("Even Branch taken as Even no.")

# Task function for the odd branch
def odd_num():
    log = LoggingMixin().log
    log.info("Odd Branch taken as Odd no.")

# Define the DAG
with DAG(
    dag_id="Dag_Branch_Python_Operator_Ex-02",              # Unique DAG ID
    description="Branch Python Operator Example 02",        # DAG description
    start_date=datetime(2025, 5, 1, tzinfo=local_tz),       # DAG start date
    schedule='@daily',                                      # Run daily
    default_args=default_args,                              # Apply default task args
    catchup=False                                           # Skip backfilling
) as dag:

    # Dummy task to indicate the start of the DAG
    start_dag = EmptyOperator(task_id='start_dag')

    # Branching decision based on the value of 'x'
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=select_branch,
        op_kwargs={'x': 28}  # Example input; change to test different paths
    )

    # Task executed if the branch condition selects "even"
    even_branch = PythonOperator(
        task_id='even_branch',
        python_callable=even_num
    )

    # Task executed if the branch condition selects "odd"
    odd_branch = PythonOperator(
        task_id='odd_branch',
        python_callable=odd_num
    )

    # Dummy task to mark the end of the DAG
    # Will run even if only one of the upstream branches runs
    end_dag = EmptyOperator(
        task_id='end_dag',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS  # Handles branching correctly
    )

    # Set task dependencies
    start_dag >> branching                              # Start → Branching
    branching >> [even_branch, odd_branch] >> end_dag   # One branch → End
