from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta

# -------------------------
# Default arguments for the DAG and its tasks
# -------------------------
default_args = {
    'owner'     : 'airflow',
    'retries'   : 0,
    'retry_delay': timedelta(minutes=7)
}

# -------------------------
# Python function to calculate square of a number
# -------------------------
def square_num(x: int):
    log = LoggingMixin().log
    y = x * x
    log.info(f"Square of {x} is {y}")

# -------------------------
# DAG definition with catchup disabled
# -------------------------
with DAG(
    dag_id="catchup_example_02",
    description="Catchup when Disabled, not by default",
    start_date=datetime(2025, 4, 25),
    schedule='@daily',
    default_args=default_args,
    catchup=False  # Catchup disabled: past runs won't be auto-created
) as dag:

    # Task: Log the square of the number 10
    task1 = PythonOperator(
        task_id="square_number",
        python_callable=square_num,
        op_kwargs={'x': 10}
    )

    task1  # No dependencies, single task
