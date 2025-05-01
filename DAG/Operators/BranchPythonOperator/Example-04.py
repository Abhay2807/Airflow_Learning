from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
import random

# Set default arguments for all tasks
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=7)
}

# Python function to randomly assign a player rating and return a task ID
def player_rating():
    x = random.randint(0, 300)  # Generate random rating between 0–300
    log = LoggingMixin().log
    log.info(f"The rating score for the player is: {x}")

    # Return specific task_id based on score
    if 200 <= x <= 300:
        return 'pro_player_task'
    elif 100 <= x < 200:
        return 'good_player_task'
    elif 50 <= x < 100:
        return 'rookie_player_task'
    else:
        return 'baby_player_task'

# Define DAG
with DAG(
    dag_id="Dag_Branch_Python_Operator_Ex-04",
    description="Branch Python Operator Example 04",
    start_date=datetime(2025, 5, 1),
    schedule='@daily',
    default_args=default_args,
    catchup=False
) as dag:

    # Starting point of the DAG
    start_task = EmptyOperator(task_id='start')

    # Branching task - decides which task to run based on score
    branch_task = BranchPythonOperator(
        task_id='branch',
        python_callable=player_rating
    )

    # Define the branch tasks
    pro_player = EmptyOperator(task_id='pro_player_task')
    good_player = EmptyOperator(task_id='good_player_task')
    rookie_player = EmptyOperator(task_id='rookie_player_task')
    baby_player = EmptyOperator(task_id='baby_player_task')

    # Join task to converge branches
    join = EmptyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS  # Ensures join runs if any one branch succeeds
    )

    # Final task
    end_task = EmptyOperator(task_id='end')

    # DAG task flow
    start_task >> branch_task
    branch_task >> [pro_player, good_player, rookie_player, baby_player] >> join
    join >> end_task
