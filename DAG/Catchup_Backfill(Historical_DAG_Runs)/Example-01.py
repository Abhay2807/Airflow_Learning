from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta

# -------------------------
# Default arguments applied to all tasks
# -------------------------
default_args = {
    'owner'     : 'airflow',
    'retries'   : 0,
    'retry_delay': timedelta(minutes=7)
}

# -------------------------
# Task function to calculate square
# -------------------------
def square_num(x: int):
    log = LoggingMixin().log
    y = x * x
    log.info(f"Square of {x} is {y}")

# -------------------------
# DAG definition
# -------------------------
with DAG(
    dag_id="catchup_example_01",
    description="Catchup when Enabled (by default also)",
    start_date=datetime(2025, 4, 25),
    schedule='@daily',
    default_args=default_args,
    catchup=True  # Enables historical runs if missed (default is True)
) as dag:

    # Task to calculate square of 10
    task1 = PythonOperator(
        task_id="square_number",
        python_callable=square_num,
        op_kwargs={'x': 10}
    )

    task1  # No downstream dependencies in this simple example
