# Importing necessary libraries
from airflow import DAG  # DAG class to create Airflow workflows
from datetime import *   # For working with date and time (e.g., start_date)
from airflow.operators.bash import BashOperator  # BashOperator to run shell commands as tasks

# Defining default arguments dictionary
default_arg = {
    'owner': 'airflow',   # Owner name; good practice for tracking responsibility
    'retries': 0          # Number of times to retry a task if it fails (0 = no retries)
}

# Defining the DAG
with DAG(
    dag_id='Bash_Operator_Dag',            # Unique identifier for the DAG
    description="A Simple Bash Operator DAG",  # Description of what the DAG does
    start_date=datetime(2025, 4, 25),       # Start date of the DAG (it won't run before this date)
    schedule='@daily',                      # How often the DAG should run (@daily = once per day)
    default_args=default_arg,               # Default arguments passed to all tasks
    catchup=False # Only run from today onward, not previous backfill
) as dag:
    
    # First task: prints a Hello World message
    task1 = BashOperator(
        task_id='Bash_Print_Out',            # Unique identifier for the task
        bash_command="echo Hello World, This is first Task"  # Command to execute
    )

    # Second task: prints another message
    task2 = BashOperator(
        task_id='Bash_Task_Two',             # Unique identifier for the task
        bash_command="echo Hello World, This is Second Task,"
                     "Me and Task three are running at same time after task1"  # Command to execute
    )

    # Third task: prints yet another message
    task3 = BashOperator(
        task_id='Bash_Task_Three',           # Unique identifier for the task
        bash_command="echo Hello World, This is Task Three, Me and Task two are running at same time after task1"  # Command to execute
    )

    # Setting up the task dependencies
    # task1 should run first, and after it succeeds, both task2 and task3 should run in parallel
    task1 >> [task2, task3]
