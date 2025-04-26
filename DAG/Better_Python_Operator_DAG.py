# Importing required classes and functions from Airflow and Python libraries
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin

# Default arguments to be applied to all tasks in the DAG
default_arg = {
    'owner': 'airflow',            # Owner of the DAG
    'retries': 0,                  # Number of retries if a task fails
    'retry_delay': timedelta(minutes=3)  # Delay between retries
}

# Function to add two numbers and log the result
def sum_numbers(a, b):
    log = LoggingMixin().log
    result = a + b
    log.info(f"The Result of {a} + {b} is = {result}")

# Function to multiply two numbers and log the result
def multiply_numbers(a, b):
    log = LoggingMixin().log
    result = a * b
    log.info(f"The Result of {a} * {b} is = {result}")

# Function to greet a user using their first and last name
def greeting_user(first, last):
    log = LoggingMixin().log
    log.info(f"Hello!, {first} {last} welcome to Apache Airflow!!!")

# Function to calculate the exponentiation of a base raised to a power and log the result
def baseExpo(base, exponent):
    log = LoggingMixin().log
    result = base ** exponent
    log.info(f"The Result of {base} raised to power {exponent} is = {result}")

# Defining the DAG
with DAG(
    dag_id='Better_Python_Operator_DAG',     # Unique identifier for the DAG
    description='Python Operator DAG with Better Code',  # Description for the DAG
    default_args=default_arg,                # Default arguments defined above
    start_date=datetime(2025, 4, 25),         # Start date for the DAG
    schedule='@daily',                       # DAG will run once every day
    catchup=False                             # No backfilling of missed DAG runs
) as dag:
    
    # Task 1: Sum two numbers using PythonOperator
    task1 = PythonOperator(
        task_id='Sum_Two_Numbers',          # Unique task id
        python_callable=sum_numbers,        # Python function to call
        op_args=[10, 18]                    # Positional arguments to pass to the function
    )

    # Task 2: Multiply two numbers using PythonOperator
    task2 = PythonOperator(
        task_id='Multply_Two_Numbers',       # Unique task id (Note: spelling mistake in 'Multiply')
        python_callable=multiply_numbers,    # Python function to call
        op_args=[12, 6]                      # Positional arguments to pass to the function
    )

    # Task 3: Greet a user using their first and last name
    task3 = PythonOperator(
        task_id='Greeting_User',              # Unique task id
        python_callable=greeting_user,        # Python function to call
        op_kwargs={'first': 'Abhay', 'last': 'Singh'}  # Keyword arguments to pass to the function
    )

    # Task 4: Calculate base exponentiation
    task4 = PythonOperator(
        task_id='Base_Exponent',               # Unique task id
        python_callable=baseExpo,              # Python function to call
        op_kwargs={'base': 2, 'exponent': 10}  # Keyword arguments to pass to the function
    )

    # Setting up task dependencies
    # task1 >> task2 >> [task3, task4] means:
    # task1 runs first -> on success, task2 runs -> then both task3 and task4 run in parallel
    task1 >> task2 >> [task3, task4]
