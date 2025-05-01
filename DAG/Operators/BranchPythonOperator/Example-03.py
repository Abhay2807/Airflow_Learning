from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
from pendulum import timezone

# Set local timezone for DAG start date
local_tz = timezone("Asia/Kolkata")

# Default arguments applied to all tasks
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=7)
}

# This function defines which branches (tasks) should be executed
def select_branches():
    # Define a list of teams that have won trophies
    team_with_trophies = ['MI', 'CSK', 'KKR']
    
    final_task_list = []

    # Convert each team to lowercase and append '_task' to match task_ids
    for team in team_with_trophies:
        task_name = team.lower() + '_task'
        final_task_list.append(task_name)

    # Return a list of task_ids to execute
    return final_task_list  # This result can be seen in XCom

# Define the DAG
with DAG(
    dag_id="Dag_Branch_Python_Operator_Ex-03",
    description="Branch Python Operator Example 03",
    start_date=datetime(2025, 5, 1, tzinfo=local_tz),
    schedule='@daily',
    default_args=default_args,
    catchup=False
) as dag:

    # Initial dummy task
    starting_task = EmptyOperator(task_id='starting_task')

    # Final dummy task
    ending_task = EmptyOperator(task_id='ending_task')

    # BranchPythonOperator to determine which tasks to run
    branch_task = BranchPythonOperator(
        task_id='branch_task',
        python_callable=select_branches
    )

    # Define individual tasks representing IPL teams
    MI = EmptyOperator(task_id='mi_task')
    CSK = EmptyOperator(task_id='csk_task')
    KKR = EmptyOperator(task_id='kkr_task')
    RCB = EmptyOperator(task_id='rcb_task')  # RCB not in the winning list

    # Join task that executes only if at least one upstream task succeeds
    joining_task = EmptyOperator(
        task_id='join',
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # Define task dependencies (DAG structure)
    starting_task >> branch_task
    branch_task >> [MI, CSK, KKR, RCB] >> joining_task
    joining_task >> ending_task
