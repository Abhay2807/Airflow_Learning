from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from pendulum import timezone

# Define timezone
my_time_zone = timezone("Asia/Kolkata")

# Define default arguments
default_args = {
    'owner': 'airflow',
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Data to pass to Dag_Number_Two
person_info = {
    'name': "Abhay Singh",
    "age": 22,
    "gender": "M",
    "place": "Chandigarh"
}

# Define DAG
with DAG(
    dag_id='Dag_Number_One',
    description='First DAG to trigger DAG 2',
    start_date=datetime(2025, 5, 3, tzinfo=my_time_zone),
    schedule='@daily',
    catchup=False,
    default_args=default_args
) as dag:

    start_task = EmptyOperator(task_id='start_task')

    # Trigger DAG 2 with personal info
    trigger_dag_two = TriggerDagRunOperator(
        task_id='trigger_dag_two',
        trigger_dag_id='Dag_Number_Two',
        conf=person_info,
        wait_for_completion=False,
        reset_dag_run=True
    )

    end_task = EmptyOperator(task_id='end_task')

    # Set task dependencies
    start_task >> trigger_dag_two >> end_task
