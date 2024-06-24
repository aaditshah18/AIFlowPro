from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
import yaml
import uuid
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 23),
}

dag = DAG(
    'setup_and_trigger_google_batch_job',
    default_args=default_args,
    schedule_interval=None,
)

def update_batch_config(**kwargs):
    unique_job_name = f"ml-training-job-{uuid.uuid4()}"
    config_path = '/tmp/batch-template.yml'
    
    with open(config_path, 'r') as file:
        config = yaml.safe_load(file)
    
    config['name'] = unique_job_name
    
    with open(config_path, 'w') as file:
        yaml.safe_dump(config, file)
    
    # Push the unique job name to XCom for later use
    kwargs['ti'].xcom_push(key='unique_job_name', value=unique_job_name)

update_batch_config_task = PythonOperator(
    task_id='update_batch_config',
    python_callable=update_batch_config,
    provide_context=True,
    dag=dag,
)

submit_batch_job = BashOperator(
    task_id='submit_batch_job',
    bash_command='gcloud batch jobs submit $(< /tmp/unique_job_name) --location=us-central1 --config=/tmp/batch-template.yml',
    dag=dag,
)

update_batch_config_task >> submit_batch_job
