from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Default arguments applied to all tasks in the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

# Message (parameters) to send to the child DAG
message_to_child = {
    "msg": "You have triggered by your Parent Dag"
}

# Define the Parent DAG
with DAG(
    dag_id='Parent_Dag_v1',
    description="This is Parent Dag v1",
    start_date=datetime(2025, 5, 1),
    default_args=default_args,
    schedule='@daily',      # This DAG runs daily
    catchup=False           # No backfill for missed runs
) as dag:

    # Dummy task marking the start of the DAG
    parent_dag_start = EmptyOperator(
        task_id='parent_dag_start'
    )

    # Task to trigger the Child DAG
    trigger_child_dag = TriggerDagRunOperator(
        task_id='trigger_child_dag',
        trigger_dag_id='Child_Dag_v1',    # ID of the child DAG to trigger
        conf=message_to_child,            # Message sent to child via conf
        reset_dag_run=True,               # If a run exists at the same logical_date, it will be replaced
        wait_for_completion=True,         # Parent will wait until child DAG finishes
        poke_interval=15,                 # Check every 15 seconds
        allowed_states=['success'],       # Consider child successful only if DAG run ends with success
        failed_states=['failed']          # Fail parent if child DAG fails
    )

    # Dummy task marking the end of the DAG
    parent_dag_end = EmptyOperator(
        task_id='parent_dag_end'
    )

    # Define task sequence
    parent_dag_start >> trigger_child_dag >> parent_dag_end
