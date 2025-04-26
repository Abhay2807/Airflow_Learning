# Importing necessary libraries
from airflow import DAG  # To create and manage DAGs
from datetime import datetime, timedelta  # For date and time functions
from airflow.operators.python import PythonOperator  # For running Python functions as tasks

# Defining default arguments for all tasks in the DAG
default_arg = {
    'owner': 'airflow',   # Owner of the DAG, useful for monitoring and responsibility
    'retries': 0          # Number of retries if a task fails (0 means no retries)
}

# Defining the Python function for the first task
def hello_world():
    # Prints a simple message; could be replaced with logging for better production use
    print("Hello World, Task 1")

# Defining the Python function for the second task
def calculate(a, b):
    # Prints the product of two numbers
    print(a * b)

# Defining the DAG
with DAG(
    dag_id='Python_Operator_DAG',          # Unique identifier for the DAG
    description="Our Python Operator DAG", # Short description of the DAG
    start_date=datetime(2025, 4, 25),       # Date from which the DAG starts running
    schedule='@daily',                      # Schedule interval - runs once daily
    default_args=default_arg,               # Attach default arguments to the DAG
    catchup=False                           # Prevents backfilling of old runs when deploying the DAG
) as dag:
    
    # Defining the first task using PythonOperator
    task1 = PythonOperator(
        task_id='First_Python_Task',        # Unique ID for the task
        python_callable=hello_world         # Reference to the function (no parentheses)
    )

    # Defining the second task using PythonOperator
    task2 = PythonOperator(
        task_id='Second_Python_Task',        # Unique ID for the task
        python_callable=calculate,           # Reference to the function (no parentheses)
        op_args=[1, 2]                       # Passing positional arguments to the function
    )

    # Setting task dependencies
    # task1 must run first, and only after it succeeds, task2 will start
    task1 >> task2
