from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pendulum import timezone

# Set the local timezone for DAG scheduling
local_tz = timezone("Asia/Kolkata")

# Define default arguments for all tasks in the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=7)
}

# Function to decide which branch to take based on input value
def select_branch(x: int):
    if x % 2 == 0:
        return 'even_branch'  # If even, return task_id of even_branch
    else:
        return 'odd_branch'   # If odd, return task_id of odd_branch

# Task function to log that even number path was taken
def even_num():
    log = LoggingMixin().log
    log.info("Even Branch taken as Even no.")

# Task function to log that odd number path was taken
def odd_num():
    log = LoggingMixin().log
    log.info("Odd Branch taken as Odd no.")

# Define the DAG
with DAG(
    dag_id="Dag_Branch_Python_Operator_Ex-01",
    description="Branch Python Operator Example 01",
    start_date=datetime(2025, 5, 1, tzinfo=local_tz),
    schedule='@daily',
    default_args=default_args,
    catchup=False
) as dag:
    
    # Start dummy task (for better DAG structure)
    start_dag = EmptyOperator(task_id='start_dag')
    
    # End dummy task (to merge branches)
    end_dag = EmptyOperator(task_id='end_dag')

    # BranchPythonOperator: decides which path to take (even_branch or odd_branch)
    branching = BranchPythonOperator(
        task_id='branching',
        python_callable=select_branch,
        op_kwargs={'x': 28}  # x = 28, so "even_branch" will be returned
    )

    # This task runs if the value is even
    even_branch = PythonOperator(
        task_id='even_branch',
        python_callable=even_num
    )

    # This task runs if the value is odd
    odd_branch = PythonOperator(
        task_id='odd_branch',
        python_callable=odd_num
    )

    # Define task dependencies
    # 1. Start DAG
    # 2. Branching decision (based on value of x)
    # 3. Only one of the branches will run
    # 4. Both branches (whichever was selected) converge at end_dag
    start_dag >> branching
    branching >> [even_branch, odd_branch] >> end_dag
