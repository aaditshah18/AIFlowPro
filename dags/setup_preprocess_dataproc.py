from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Import the preprocess_data function
from tasks import preprocess_data

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'preprocess_data_dag',
    default_args=default_args,
    description='Run preprocessing script',
    schedule_interval=None,  # Triggered manually or via Cloud Function
)

# Task to run the preprocessing script
run_preprocessing = PythonOperator(
    task_id='run_preprocessing',
    python_callable=preprocess_data,
    dag=dag,
)

run_preprocessing
