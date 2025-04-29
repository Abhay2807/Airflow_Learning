from airflow.decorators import dag, task
from airflow.utils.log.logging_mixin import LoggingMixin
from datetime import datetime, timedelta

# Define default arguments for the DAG.  These arguments are applied to all tasks in the DAG,
# unless overridden by specific task arguments.
default_args = {
    'owner': 'airflow',  # The owner of the DAG.  Useful for tracking and permissions.
    'retries': 0,        # Number of times a task should retry if it fails.
    'retry_delay': timedelta(minutes=5)  # Time delay between retries.
}

# Define the DAG using the @dag decorator.  This decorator transforms the following
# function into an Airflow DAG object.
@dag(
    dag_id='Taskflow_DAG_Eg_03',  # Unique identifier for the DAG.
    description='Taskflow DAG Example-03',  # A descriptive text for the DAG.
    catchup=False,  # Whether or not the DAG should catch up on past scheduled runs.  False is common for production DAGs.
    start_date=datetime(2025, 4, 25),  # The date at which the DAG starts running.
    schedule='@daily',  # How often the DAG should run.  '@daily' means once per day.
    default_args=default_args  # Apply the default arguments defined above.
)
def taskflow_dag_v3():
    """
    This DAG demonstrates the TaskFlow API in Airflow for defining tasks and their dependencies.
    It calculates the square and cube of a number, and then performs a summation.
    """

    # Define a task to square a given number using the @task decorator.
    @task
    def square_num(x: int):
        """
        Squares the input number.

        Args:
            x: The number to be squared.

        Returns:
            The square of the number.
        """
        return (x**2)

    # Define a task to cube a given number.
    @task
    def cube_num(y: int):
        """
        Cubes the input number.

        Args:
            y: The number to be cubed.

        Returns:
            The cube of the number.
        """
        return (y**3)

    # Define a task that returns multiple outputs as a dictionary.  The multiple_outputs=True
    # argument tells Airflow that this task will return a dictionary, and the keys of the
    # dictionary should be treated as separate outputs.
    @task(multiple_outputs=True)
    def numbers():
        """
        Returns a dictionary containing two numbers.

        Returns:
            A dictionary with keys 'a' and 'b', and corresponding integer values.
        """
        return {
            'a': 10,
            'b': 20
        }

    # Define a task that performs calculations and logs the results.
    @task
    def calculate(a, b, p, c, d):
        """
        Calculates the sum of two numbers and logs intermediate results.

        Args:
            a: The square of a number.
            b: The cube of a number.
            p: The original number.
            c: The first number to be summed.
            d: The second number to be summed.
        """
        log = LoggingMixin().log  # Get the Airflow logger.
        log.info(f"Square of {p} is {a}")  # Log the square of p.
        log.info(f"Cube of {a} is {b}")  # Log the cube of a.
        result = c + d  # Calculate the sum of c and d.
        log.info(f"Sum of {c} and {d} is : {result}")  # Log the sum.

    # Define an initial number.
    p = 2
    # Call the square_num task with the initial number, and store the result in num1.
    num1 = square_num(p)
    # Call the cube_num task with the result from square_num, and store the result in num2.
    num2 = cube_num(num1)
    # Call the numbers task to get the dictionary of numbers.
    values = numbers()
    # Call the calculate task with the results from the previous tasks and the values from the numbers task.
    calculate(num1, num2, p, c=values['a'], d=values['b'])

# Instantiate the DAG.  This creates an instance of the DAG defined by the
# taskflow_dag_v3 function.  This instance is what Airflow uses to schedule
# and run the workflow.
taskflow_dag_eg_03 = taskflow_dag_v3()
