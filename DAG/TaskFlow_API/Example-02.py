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
    dag_id='Taskflow_DAG_Eg_02',
    description="Taskflow DAG Example-02",
    start_date=datetime(2025, 4, 25),
    schedule='@daily',
    catchup=False,
    default_args=default_args
)
def taskflow_dag_v2():

    # Task: Extracts data (a list of integers)
    @task
    def extract():
        return [1, 2, 3]
    
    # Task: Transforms the extracted data (squares each element)
    @task
    def transform(data):
        return [x**2 for x in data]
    
    # Task: Loads the transformed data (logs it)
    @task
    def load(data):
        log = LoggingMixin().log
        log.info(f"Data is: {data}")

    # Define the task dependencies and data flow
    data             = extract()           # Extract data
    transformed_data = transform(data)      # Transform the extracted data
    load(transformed_data)                  # Load (log) the transformed data

# Instantiate the DAG
taskflow_dag_eg_02 = taskflow_dag_v2()
