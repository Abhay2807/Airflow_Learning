# Import necessary libraries and modules
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

# Define default arguments for the DAG
default_args = {
    'owner'      : 'airflow',
    'retries'    : 0,
    'retry_delay': timedelta(minutes=3)
}

# Define the DAG using the @dag decorator
@dag(
    dag_id='Taskflow_DAG_Eg_01',
    description="Taskflow DAG Example-01",
    start_date=datetime(2025, 4, 25),
    schedule='@daily',
    catchup=False,
    default_args=default_args
)
def taskflow_dag_v1():
    
    # Task 1: Logs a greeting message and returns a greeting string
    @task
    def task1():
        log = LoggingMixin().log
        log.info(f"Hello Abhay Singh!")
        return "Greeting from Task 1"  # Return a value

    # Task 2: Returns a name
    @task
    def task2():
        return "Abhay Singh"

    # Task 3: Returns an age
    @task
    def task3():
        return 22

    # Task 4: Returns college details as a dictionary (multiple outputs)
    @task(multiple_outputs=True)
    def task4():
        return {
            'College': 'NIT Hamirpur',
            'CGPA'   : 9.56
        }

    # Task 5: Logs multiple pieces of information using inputs from other tasks
    @task
    def task5(name, age, greeting, college, cgpa):
        log = LoggingMixin().log
        log.info(f"{greeting}")
        log.info(f"My name is {name} aged {age} years old!")
        log.info(f"My College is {college}, passed with an C.G.P.A {cgpa}")

    # Define the task dependencies and capture outputs
    greeting_message = task1()  # Capture the return value of task1
    name             = task2()  # Capture the return value of task2
    age              = task3()  # Capture the return value of task3
    college_details  = task4()  # Capture the return value of task4 (as a dictionary)

    # Pass the captured outputs to task5
    task5(
        name,
        age,
        greeting_message,
        college=college_details['College'],
        cgpa=college_details['CGPA']
    )

# Instantiate the DAG
taskflow_dag_eg_01 = taskflow_dag_v1()
