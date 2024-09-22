from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import subprocess

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 9, 22),
    'retries': 1,
}

# Define the function that runs the external Python script
def run_script():
    subprocess.run(['python', 'E:/DE/Projects/Python & API Data/YT API Data extraction and edt/yt_extractor.py'], check=True)

# Define the DAG
with DAG('yt_extractor', default_args=default_args, schedule_interval='@daily', catchup=False) as dag:

    # Use PythonOperator to run the entire Python script
    run_python_script = PythonOperator(
        task_id='yt_extractor',
        python_callable=run_script
    )

    # Define task dependencies if necessary
    run_python_script
