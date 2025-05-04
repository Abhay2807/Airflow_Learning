from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from pendulum import timezone

# Logger setup
log = LoggingMixin().log

# Set timezone to IST
my_time_zone = timezone("Asia/Kolkata")

# Dummy function to simulate a task
def dummy_task(task_name):
    print(f"Running {task_name}")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Define the DAG
with DAG(
    dag_id='Grouping_Tasks_DAG',
    description='Tasks grouping DAG Example',
    start_date=datetime(2025, 5, 4, tzinfo=my_time_zone),
    schedule=None,                # DAG is triggered manually (no automatic schedule)
    catchup=False,                # Do not backfill past runs
    default_args=default_args
) as dag:

    # Dummy starting point
    start_task = EmptyOperator(
        task_id='start_task'
    )

    # Dummy ending point
    end_task = EmptyOperator(
        task_id='end_task'
    )

    # TaskGroup for Extraction Tasks
    with TaskGroup("extract_tasks", tooltip="Extract from sources") as extract_group:
        extract_a = PythonOperator(
            task_id='extract_a',
            python_callable=lambda: dummy_task("Extract A"),
        )
        extract_b = PythonOperator(
            task_id='extract_b',
            python_callable=lambda: dummy_task("Extract B"),
        )
        extract_c = PythonOperator(
            task_id='extract_c',
            python_callable=lambda: dummy_task("Extract C"),
        )

    # TaskGroup for Transformation Tasks
    with TaskGroup("transform_tasks", tooltip="Transform data") as transform_group:
        transform_a = PythonOperator(
            task_id='transform_a',
            python_callable=lambda: dummy_task("Transform A"),
        )
        transform_b = PythonOperator(
            task_id='transform_b',
            python_callable=lambda: dummy_task("Transform B"),
        )
        transform_c = PythonOperator(
            task_id='transform_c',
            python_callable=lambda: dummy_task("Transform C"),
        )

    # TaskGroup for Load Tasks
    with TaskGroup("load_tasks", tooltip="Load data") as load_group:
        load_a = PythonOperator(
            task_id='load_a',
            python_callable=lambda: dummy_task("Load A"),
        )
        load_b = PythonOperator(
            task_id='load_b',
            python_callable=lambda: dummy_task("Load B"),
        )
        load_c = PythonOperator(
            task_id='load_c',
            python_callable=lambda: dummy_task("Load C"),
        )

    # Define task dependencies
    start_task >> extract_group >> transform_group >> load_group >> end_task
