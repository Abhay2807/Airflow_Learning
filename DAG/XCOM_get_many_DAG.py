# Import necessary libraries
from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.utils.log.logging_mixin import LoggingMixin
from airflow.models import XCom
from airflow import settings  # ✔️ Import airflow settings for Session

# Define default arguments
default_arg = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=3)  # corrected key name 'retry_delay'
}

# Function to push player details to XCom
def player_details(ti):
    player_info = {
        'name': 'Rohit Sharma',
        'age': 37,
        'place': 'Mumbai'
    }
    ti.xcom_push(key='player_details', value=player_info)  # ✔️ Push dictionary, not string

# Function to push team details to XCom
def team_details(ti):
    team_info = {
        'name': 'Mumbai Indians',
        'titles': 5
    }
    ti.xcom_push(key='team_details', value=team_info)  # ✔️ Push dictionary, not string

# Function to retrieve details and log final IPL draft information
def ipl_draft(year, ti):
    
    # Create a session manually
    session = settings.Session()  # ✔️ Correct way to get session

    # Fetch XComs using get_many
		# Only for Versions older than Airflow 3.0
    xcom_records = XCom.get_many(
        execution_date=ti.execution_date,
        dag_ids=['XCOM_get_many_DAG_v1'],
        task_ids=['player', 'team'],  # ✔️ correct task_ids
        session=session
    )

    team_info = {}
    player_info = {}

    for record in xcom_records:
        if record.task_id == 'player':
            player_info = record.value
        elif record.task_id == 'team':
            team_info = record.value

    # Safely get values from dictionaries
    nm = player_info.get('name', 'Unknown')
    ag = player_info.get('age', 'Unknown')
    pl = player_info.get('place', 'Unknown')
    tn = team_info.get('name', 'Unknown')
    chmp = team_info.get('titles', 'Unknown')

    # Log the final drafted information
    log = LoggingMixin().log
    log.info(f"Player {nm} aged {ag} years Old from {pl}\n"
             f"will play for {tn} in IPL {year} which has already won {chmp} titles."
             )

# Define the DAG
with DAG(
    dag_id='XCOM_get_many_DAG_v1',
    description='DAG to pull multiple values from XCom at once',
    default_args=default_arg,
    start_date=datetime(2025, 4, 25),
    schedule='@daily',
    catchup=False
) as dag:
    
    # Task to push player details
    task1 = PythonOperator(
        task_id='player',
        python_callable=player_details
    )

    # Task to push team details
    task2 = PythonOperator(
        task_id='team',
        python_callable=team_details
    )

    # Task to retrieve and log final IPL draft
    task3 = PythonOperator(
        task_id='ipl',
        python_callable=ipl_draft,
        op_kwargs={'year': 2025}
    )

    # Setting task dependencies
    task1 >> task2 >> task3
