from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=1)
}

# Configuration dictionary to send message to Child DAG 1
message_to_child1 = {
    "msg": "You child1 have been triggered by your Parent Dag"
}

# Configuration dictionary to send message to Child DAG 2
message_to_child2 = {
    "msg": "You child2 been have triggered by your Parent Dag"
}

# Define the Parent DAG
with DAG(
    dag_id='Parent_Dag_v2',                        # Unique ID for the DAG
    description="This is Parent Dag v2",           # Description
    start_date=datetime(2025, 5, 1),               # Start date
    default_args=default_args,                     # Pass default arguments
    schedule='@daily',                             # Run daily
    catchup=False                                  # Skip backfilling
) as dag:

    # Dummy operator to mark the start of the parent DAG
    parent_dag_start = EmptyOperator(
        task_id='parent_dag_start'
    )

    # Dummy operator to mark the end of the parent DAG
    parent_dag_end = EmptyOperator(
        task_id='parent_dag_end'
    )

    # Trigger Child DAG 1 with message
    trigger_child_dag1 = TriggerDagRunOperator(
        task_id='trigger_child_dag1',              # Task ID
        trigger_dag_id='Child_Dag_v1',             # ID of the DAG to trigger
        conf=message_to_child1,                    # Message passed to the DAG
        reset_dag_run=True,                        # Reset previous DAG run if exists
        wait_for_completion=True,                  # Wait for child DAG to complete
        poke_interval=15,                          # Polling interval to check status
        allowed_states=['success'],                # Only proceed if child DAG is successful
        failed_states=['failed']                   # Fail if child DAG fails
    )

    # Trigger Child DAG 2 with message
    trigger_child_dag2 = TriggerDagRunOperator(
        task_id='trigger_child_dag2',
        trigger_dag_id='Child_Dag_v2',
        conf=message_to_child2,
        reset_dag_run=True,
        wait_for_completion=True,
        poke_interval=15,
        allowed_states=['success'],
        failed_states=['failed']
    )

    # Set task dependencies
    parent_dag_start >> trigger_child_dag1 >> trigger_child_dag2 >> parent_dag_end
