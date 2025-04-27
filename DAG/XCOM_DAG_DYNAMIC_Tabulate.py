# Works for Airflow 3.0 Version
# Imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta
from tabulate import tabulate  # Optional but makes logs pretty!

# Default Args
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=3)
}

# Push Player Info
def player_details(ti):
    player_info = {
        'name': 'Rohit Sharma',
        'age': 37,
        'place': 'Mumbai'
    }
    ti.xcom_push(key='player_info', value=player_info)

# Push Team Info
def team_details(ti):
    team_info = {
        'name': 'Mumbai Indians',
        'titles': 5
    }
    ti.xcom_push(key='team_info', value=team_info)

# Dynamic Pull and Pretty Log
def ipl_draft_dynamic(year, ti):
    log = LoggingMixin().log

    # Pull from XComs
    player_info = ti.xcom_pull(task_ids='player', key='player_info')
    team_info = ti.xcom_pull(task_ids='team', key='team_info')

    combined_info = {
        "Player Name": player_info.get('name', 'N/A'),
        "Player Age": player_info.get('age', 'N/A'),
        "Player Place": player_info.get('place', 'N/A'),
        "Team Name": team_info.get('name', 'N/A'),
        "Team Titles": team_info.get('titles', 'N/A'),
        "Draft Year": year
    }

    # Beautiful Table
    table = tabulate(combined_info.items(), headers=["Field", "Value"], tablefmt="grid")
    log.info(f"🎯 IPL Draft Information:\n{table}")

# Define DAG
with DAG(
    dag_id='XCOM_get_many_DAG_dynamic',
    description='DAG with dynamic XCom pulling and pretty logging',
    default_args=default_args,
    start_date=datetime(2025, 4, 25),
    schedule_interval='@daily',
    catchup=False
) as dag:

    task1 = PythonOperator(
        task_id='player',
        python_callable=player_details
    )

    task2 = PythonOperator(
        task_id='team',
        python_callable=team_details
    )

    task3 = PythonOperator(
        task_id='ipl',
        python_callable=ipl_draft_dynamic,
        op_kwargs={'year': 2025}
    )

    task1 >> task2 >> task3
