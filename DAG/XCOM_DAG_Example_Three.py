# Importing necessary libraries
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.log.logging_mixin import LoggingMixin

# Default arguments for the DAG
default_arg = {
    'owner': 'airflow',                         # Owner of the DAG
    'retries': 0,                               # Number of retries if a task fails
    'retry_delay': timedelta(minutes=3)         # Delay between retries
}

# Function to push first name and last name into XCom
def name_info(ti):
    first_name = "Abhay"
    last_name = "Singh"
    ti.xcom_push(key='first_name', value=first_name)    # Push first name
    ti.xcom_push(key='last_name', value=last_name)      # Push last name

# Function to push job-related information into XCom
def job_info(ti):
    ti.xcom_push(key='company', value='HDFC Bank')      # Push company name
    ti.xcom_push(key='workEx', value='22')              # Push work experience in months
    ti.xcom_push(key='Role', value='Data Engineer')     # Push job role

# Function to retrieve all information from XCom and log it
def retrieve_employee_info(age, ti):
    # Pulling name-related information from first task
    fn = ti.xcom_pull(task_ids='first_task_name', key='first_name')
    ln = ti.xcom_pull(task_ids='first_task_name', key='last_name')

    # Pulling job-related information from second task
    cmp = ti.xcom_pull(task_ids='second_task_job', key='company')
    we = ti.xcom_pull(task_ids='second_task_job', key='workEx')
    rl = ti.xcom_pull(task_ids='second_task_job', key='Role')

    # Logging the collected employee information
    log = LoggingMixin().log
    log.info(
        f"Employee: {fn} {ln}, Age: {age} years old\n"
        f"Company: {cmp}, Work Experience: {we} months, Role: {rl}"
    )

# Defining the DAG
with DAG(
    dag_id='XCOM_DAG_Example_Three',                 # Unique identifier for the DAG
    description='Python Operator DAG with Better Code',  # Description for the DAG
    default_args=default_arg,                        # Default arguments defined above
    start_date=datetime(2025, 4, 25),                 # Start date for the DAG
    schedule='@daily',                               # DAG will run once every day
    catchup=False                                    # No backfilling of missed DAG runs
) as dag:
    
    # Task 1: Push name information into XCom
    name = PythonOperator(
        task_id='first_task_name',
        python_callable=name_info
    )

    # Task 2: Push job information into XCom
    job = PythonOperator(
        task_id='second_task_job',
        python_callable=job_info
    )

    # Task 3: Retrieve information from XCom and log it
    retrieve_info = PythonOperator(
        task_id='third_task_retrieve',
        python_callable=retrieve_employee_info,
        op_kwargs={'age': 22}        # Pass 'age' as a keyword argument
    )

    # Setting task dependencies: name -> job -> retrieve_info
    name >> job >> retrieve_info

# ------------------------------------------------------------------------------------------
# SUMMARY:
# - Task 1 (first_task_name) pushes first name and last name into XCom.
# - Task 2 (second_task_job) pushes company, work experience, and role into XCom.
# - Task 3 (third_task_retrieve) pulls all above values from XCom and logs a detailed employee summary.
# - The flow is strictly sequential: first_task_name -> second_task_job -> third_task_retrieve.
# ------------------------------------------------------------------------------------------
