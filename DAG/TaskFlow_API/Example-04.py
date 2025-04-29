from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.utils.log.logging_mixin import LoggingMixin

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=3)
}

@dag(
    dag_id='taskflow_return_comparison_fixed',
    start_date=datetime(2025, 4, 28),
    schedule='@daily',
    catchup=False,
    default_args=default_args
)
def return_handling_dag():

    # --- Tuple Return ---
    @task
    def return_tuple():
        return 100, 200  # returns a tuple

    @task
    def handle_tuple(t):
        val1, val2 = t  # safe unpacking at runtime
        log = LoggingMixin().log
        log.info(f"[Tuple Style] Value 1 = {val1}, Value 2 = {val2}")

    # --- Dict Return with multiple_outputs ---
    @task(multiple_outputs=True)
    def return_dict():
        return {
            'val1': 300,
            'val2': 400
        }

    @task
    def handle_dict(val1, val2):
        log = LoggingMixin().log
        log.info(f"[Dict Style] Value 1 = {val1}, Value 2 = {val2}")

    # Execute tasks
    t = return_tuple()
    handle_tuple(t)

    vals = return_dict()
    handle_dict(vals['val1'], vals['val2'])

# Instantiate DAG
taskflow_return_comparison_fixed = return_handling_dag()
