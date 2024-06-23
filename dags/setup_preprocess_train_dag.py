from airflow import DAG
from airflow.providers.google.cloud.operators.compute import (
    ComputeEngineStartInstanceOperator,
    ComputeEngineStopInstanceOperator,
    ComputeEngineInsertInstanceOperator,
    ComputeEngineDeleteInstanceOperator,
)
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.ssh.operators.ssh import SSHOperator
from airflow.providers.google.cloud.transfers.local_to_gcs import (
    LocalFilesystemToGCSOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable, Connection
from dags.tasks import (
    create_ssh_connection,
    download_code_from_gcs,
    preprocess_data,
    send_email,
    get_external_ip,
    train_model,
)
import json
from airflow import settings

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

PROJECT_ID = Variable.get("PROJECT_ID")
ZONE = Variable.get("ZONE")  # us-central1-a
REGION = Variable.get("REGION").strip()  # us-central1
INSTANCE_NAME = Variable.get("GCE_INSTANCE_NAME")
CODE_BUCKET_NAME = Variable.get("CODE_BUCKET_NAME")
MODEL_BUCKET_NAME = 'your-model-bucket'
CODE_PATH = Variable.get("CODE_PATH")
MODEL_PATH = 'path/to/model'
MODEL_FILE = 'model.pkl'
SOURCE_IMAGE = 'projects/debian-cloud/global/images/family/debian-10'
SHORT_MACHINE_TYPE_NAME = Variable.get('MACHINE_TYPE').strip()  # e2-highmem-4

STARTUP_SCRIPT = """
#!/bin/bash
apt-get update
apt-get install -y software-properties-common
add-apt-repository ppa:deadsnakes/ppa
apt-get update
apt-get install -y python3.9 python3.9-distutils python3.9-venv
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python3.9 get-pip.py
pip3.9 install pandas numpy matplotlib seaborn scikit-learn xgboost
"""

GCE_INSTANCE_BODY = {
    "name": INSTANCE_NAME,
    "machine_type": f"zones/{ZONE}/machineTypes/{SHORT_MACHINE_TYPE_NAME}",
    "disks": [
        {
            "boot": True,
            "device_name": INSTANCE_NAME,
            "initialize_params": {
                "disk_size_gb": "10",
                "disk_type": f"zones/{ZONE}/diskTypes/pd-balanced",
                "source_image": "projects/debian-cloud/global/images/debian-11-bullseye-v20220621",
            },
        }
    ],
    "network_interfaces": [
        {
            "access_configs": [{"name": "External NAT", "network_tier": "PREMIUM"}],
            "stack_type": "IPV4_ONLY",
            "subnetwork": f"regions/{REGION}/subnetworks/default",
        }
    ],
    "metadata": {
        "items": [
            {
                "key": "startup-script",
                "value": STARTUP_SCRIPT,
            }
        ]
    },
}

# Task to create a new VM instance
create_vm_task = ComputeEngineInsertInstanceOperator(
    task_id='create_vm',
    project_id=PROJECT_ID,
    zone=ZONE,
    body=GCE_INSTANCE_BODY,
    dag=dag,
)

# Task to start the VM if it already exists
start_vm_task = ComputeEngineStartInstanceOperator(
    task_id='start_vm',
    project_id=PROJECT_ID,
    zone=ZONE,
    resource_id=INSTANCE_NAME,
    dag=dag,
)

# Task to get the username of the current user in the VM
get_username_task = BashOperator(
    task_id='get_username',
    bash_command='whoami',
    do_xcom_push=True,
    dag=dag,
)

# # Task to get the external IP of the VM and set up SSH connection
# get_external_ip_task = PythonOperator(
#     task_id='get_external_ip',
#     python_callable=get_external_ip,
#     provide_context=True,
#     dag=dag,
# )

# # Task to generate SSH key inside the VM
# generate_ssh_key_task = SSHOperator(
#     task_id='generate_ssh_key',
#     ssh_conn_id='dynamic_ssh_connection',
#     command="""
#     mkdir -p ~/.ssh &&
#     if [ ! -f ~/.ssh/id_rsa ]; then
#         ssh-keygen -t rsa -b 2048 -f ~/.ssh/id_rsa -q -N "" &&
#         cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
#     fi
#     """,
#     dag=dag,
# )
# # Task to create the SSH connection dynamically
# create_ssh_connection_task = PythonOperator(
#     task_id='create_ssh_connection',
#     python_callable=create_ssh_connection,
#     provide_context=True,
#     dag=dag,
# )

# Task to download the latest code from GCS to the VM
download_code_task = PythonOperator(
    task_id='download_code',
    python_callable=download_code_from_gcs,
    provide_context=True,
    dag=dag,
)


# Task to preprocess data on the VM
preprocess_task = PythonOperator(
    task_id='preprocess_data',
    python_callable=preprocess_data,
    provide_context=True,
    dag=dag,
)

# Task to train the model on the VM
train_task = PythonOperator(
    task_id='train_model',
    python_callable=train_model,
    provide_context=True,
    dag=dag,
)

# Task to upload the trained model to GCS
upload_model_to_gcs_task = LocalFilesystemToGCSOperator(
    task_id='upload_model_to_gcs',
    src='/home/`{{ ti.xcom_pull(task_ids="get_username") }}`/Desktop/modelling-mlops/model.pkl',
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

# Task to send success email notification
send_success_email = PythonOperator(
    task_id='send_success_email',
    python_callable=send_email,
    op_args=['success'],
    trigger_rule='all_success',  # Only send email if the previous tasks were successful
    dag=dag,
)

# Task to send failure email notification
send_failure_email = PythonOperator(
    task_id='send_failure_email',
    python_callable=send_email,
    op_args=['failure'],
    trigger_rule='one_failed',  # Send email if any of the previous tasks failed
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

# Task to delete the VM if send_failure_email succeeds
delete_vm_task = ComputeEngineDeleteInstanceOperator(
    task_id='delete_vm',
    project_id=PROJECT_ID,
    zone=ZONE,
    resource_id=INSTANCE_NAME,
    trigger_rule='all_success',  # Ensure this task runs only if send_failure_email succeeds
    dag=dag,
)

# Define task dependencies
(
    create_vm_task
    >> start_vm_task
    >> get_username_task
    # >> get_external_ip_task
    # >> generate_ssh_key_task
    # >> create_ssh_connection_task
    >> download_code_task
    >> preprocess_task
    >> train_task
    >> upload_model_to_gcs_task
)
upload_model_to_gcs_task >> check_model_upload >> [send_success_email, trigger_next_dag]
check_model_upload >> stop_vm_task
check_model_upload >> send_failure_email >> delete_vm_task

create_vm_task >> start_vm_task
