from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
from tabulate import tabulate

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=2)
}

# Function to define and push name details to XCom
def name_details(ti):
    """
    This function defines the name and age of a student
    and pushes this information to XCom.

    Args:
        ti: The task instance object.
    """
    name_info = {
        'name': "Abhay Singh",
        'age': 22
    }
    ti.xcom_push(key='name_info', value=name_info)

# Function to define and push college details to XCom
def college_details(ti):
    """
    This function defines the college and course of a student
    and pushes this information to XCom.

    Args:
        ti: The task instance object.
    """
    college_info = {
        'college': 'NITH',
        'course': 'ECE'
    }
    ti.xcom_push(key='college_info', value=college_info)

# Function to retrieve information from XCom and log it
def retrieve_info(year, ti):
    """
    This function retrieves student name and college details from XCom,
    combines them with the passing year, and logs the final information
    in a table format.

    Args:
        year: The passing year of the student.
        ti: The task instance object.
    """
    name_info    = ti.xcom_pull(task_ids='name', key='name_info')
    college_info = ti.xcom_pull(task_ids='College', key='college_info')

    final_info = {
        "Student Name":  name_info.get("name", "N/A"),
        "Student Age":   name_info.get("age", "N/A"),
        "College":       college_info.get("college", "N/A"),
        "Programme":     college_info.get("course", "N/A"),
        "Passing year":  year
    }

    table = tabulate(final_info.items(), headers=["Details", "Value"], tablefmt="grid")
    log   = LoggingMixin().log
    log.info(f"Final Details for the Student are:\n{table}")

# Define the DAG
with DAG(
    dag_id='Tabulate_DAG_XCOM',
    start_date=datetime(2025, 4, 25),
    schedule='@daily',
    default_args=default_args,
    catchup=False
) as dag:

    # Task to get name details
    task1 = PythonOperator(
        task_id="name",
        python_callable=name_details
    )

    # Task to get college details
    task2 = PythonOperator(
        task_id="College",
        python_callable=college_details
    )

    # Task to retrieve and display the final information
    task3 = PythonOperator(
        task_id='final_info',
        python_callable=retrieve_info,
        op_kwargs={'year': 2023}
    )

    # Set up task dependencies
    task1 >> task2 >> task3
