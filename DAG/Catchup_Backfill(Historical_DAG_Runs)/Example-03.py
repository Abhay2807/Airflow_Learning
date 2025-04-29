from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta

# -------------------------
# Default arguments for the DAG and tasks
# -------------------------
default_args = {
    'owner'     : 'airflow',
    'retries'   : 0,
    'retry_delay': timedelta(minutes=7)
}

# -------------------------
# Python function to log square of a number
# -------------------------
def square_num(x: int):
    log = LoggingMixin().log
    y = x * x
    log.info(f"Square of {x} is {y}")

# -------------------------
# DAG with catchup disabled, to demonstrate manual backfill
# -------------------------
with DAG(
    dag_id="backfill_example_03",
    description="Backfill does not depend on catchup",
    start_date=datetime(2025, 4, 25),
    schedule='@daily',
    default_args=default_args,
    catchup=False  # Even though catchup is off, we can still manually backfill
) as dag:

    # Task: Print square of 10
    task1 = PythonOperator(
        task_id="square_number",
        python_callable=square_num,
        op_kwargs={'x': 10}
    )

    task1  # No downstream dependencies
