from airflow import DAG
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
)
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.email import EmailOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'setup_preprocess_train_dag',
    default_args=default_args,
    description='Setup VM, download code, preprocess data, train model, and upload to GCS',
    schedule_interval=None,  # Triggered manually or via Cloud Function
)

PROJECT_ID = Variable.get("gcp_project_id")
ZONE = Variable.get("gcp_zone")
INSTANCE_NAME = Variable.get("gce_instance_name")
CODE_BUCKET_NAME = 'your-code-bucket'
MODEL_BUCKET_NAME = 'your-model-bucket'
CODE_PATH = 'path/to/code'
MODEL_PATH = 'path/to/model'
MODEL_FILE = 'model.pkl'
MACHINE_TYPE = 'n1-standard-4'
SOURCE_IMAGE = 'projects/debian-cloud/global/images/family/debian-10'
EMAIL = 'your-email@example.com'

# Task to start the VM
start_vm_task = ComputeEngineStartInstanceOperator(
    task_id='start_vm',
    project_id=PROJECT_ID,
    zone=ZONE,
    resource_id=INSTANCE_NAME,
    dag=dag,
)

# Task to download the latest code from GCS to the VM
download_code_task = SSHOperator(
    task_id='download_code',
    ssh_conn_id='your-ssh-connection-id',
    command=f'gsutil -m cp -r gs://{CODE_BUCKET_NAME}/{CODE_PATH}/* /path/to/destination',
    dag=dag,
)

# Task to preprocess data on the VM
preprocess_task = SSHOperator(
    task_id='preprocess_data',
    ssh_conn_id='your-ssh-connection-id',
    command='python /path/to/destination/preprocess_data.py',
    dag=dag,
)

# Task to train the model on the VM
train_task = SSHOperator(
    task_id='train_model',
    ssh_conn_id='your-ssh-connection-id',
    command='python /path/to/destination/train_model.py',
    dag=dag,
)

# Task to upload the trained model to GCS
upload_model_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id='upload_model_to_gcs',
    src='/path/to/destination/model.pkl',
    dst=f'{MODEL_PATH}/{MODEL_FILE}',
    bucket=MODEL_BUCKET_NAME,
    dag=dag,
)

# Sensor to check if the model file exists in GCS
check_model_upload = GCSObjectExistenceSensor(
    task_id='check_model_upload',
    bucket=MODEL_BUCKET_NAME,
    object=f'{MODEL_PATH}/{MODEL_FILE}',
    timeout=600,
    poke_interval=60,
    dag=dag,
)

# Task to send email notification
send_email_notification = EmailOperator(
    task_id='send_email_notification',
    to=EMAIL,
    subject='Model Upload Status',
    html_content='The model file has been successfully uploaded to GCS.',
    trigger_rule='all_success',  # Only send email if the previous tasks were successful
    dag=dag,
)

send_email_failure_notification = EmailOperator(
    task_id='send_email_failure_notification',
    to=EMAIL,
    subject='Model Upload Failure',
    html_content='The model file upload to GCS has failed.',
    trigger_rule='all_failed',  # Send email if any of the previous tasks failed
    dag=dag,
)

# Task to trigger the second DAG
trigger_next_dag = TriggerDagRunOperator(
    task_id='trigger_train_and_deploy_dag',
    trigger_dag_id='train_and_deploy_dag',
    dag=dag,
)

# Task to stop the VM after the work is done
stop_vm_task = ComputeEngineStopInstanceOperator(
    task_id='stop_vm',
    project_id=PROJECT_ID,
    zone=ZONE,
    resource_id=INSTANCE_NAME,
    dag=dag,
)

# Define the task dependencies
(
    start_vm_task
    >> download_code_task
    >> preprocess_task
    >> train_task
    >> upload_model_to_gcs_task
)
(
    upload_model_to_gcs_task
    >> check_model_upload
    >> [send_email_notification, send_email_failure_notification]
    >> trigger_next_dag
)
check_model_upload >> stop_vm_task