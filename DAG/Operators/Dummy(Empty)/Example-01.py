from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.empty import EmptyOperator  # Used as DummyOperator in newer versions
from datetime import datetime, timedelta
from pendulum import timezone

# Set local timezone
local_tz = timezone("Asia/Kolkata")

# Default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=7)
}

# Python callable to calculate square of a number
def square_num(x: int):
    log = LoggingMixin().log
    y = x * x
    log.info(f"Square of {x} is {y}")

# Python callable to calculate cube of a number
def cube_num(x: int):
    log = LoggingMixin().log
    z = x ** 3
    log.info(f"Cube of {x} is {z}")

# Define the DAG
with DAG(
    dag_id="Dag_Dummy_Opr_Example-01",
    description="Dummy Operator Example 01",
    start_date=datetime(2025, 5, 1, tzinfo=local_tz),
    schedule='@daily',
    default_args=default_args,
    catchup=False
) as dag:

    # Dummy start task
    start_dag = EmptyOperator(task_id='start')

    # Task to calculate square of a number
    task1 = PythonOperator(
        task_id="square_number",
        python_callable=square_num,
        op_kwargs={'x': 10}
    )

    # Task to calculate cube of a number
    task2 = PythonOperator(
        task_id="cube_number",
        python_callable=cube_num,
        op_kwargs={'x': 10}
    )

    # Dummy end task
    end_dag = EmptyOperator(task_id='end')

    # Define task dependencies
    start_dag >> [task1, task2] >> end_dag
