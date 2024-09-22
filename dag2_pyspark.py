from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

def hello_world():
    print("Hello, World! This is just an example.")

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

# Define the DAG
with DAG(
    'hello_world_dag',
    default_args=default_args,
    description='A simple hello world DAG',
    schedule_interval='@daily',  # Schedule to run once every day
    catchup=False,
) as dag:

    # Create a PythonOperator to run the hello_world function
    hello_task = PythonOperator(
        task_id='hello_world_task',
        python_callable=hello_world,
    )

# DAG execution starts with the hello_task
hello_task
