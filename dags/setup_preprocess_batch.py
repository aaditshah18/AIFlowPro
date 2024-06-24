from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable
from dags.custom_operators import BatchJobSensor
from dags.tasks import (
    create_batch_job,
    get_latest_image_tag,
    send_email,
    submit_batch_job,
)
from airflow.providers.google.cloud.operators.vertex_ai import (
    UploadModelOperator,
    CreateEndpointOperator,
    DeployModelOperator,
)

REGION = Variable.get("REGION")
PROJECT_ID = Variable.get("PROJECT_ID")
MODEL_BUCKET_NAME = Variable.get("MODEL_BUCKET_NAME")
MODEL_PATH = Variable.get("MODEL_PATH")
MODEL_FILE = Variable.get("MODEL_FILE")

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'batch_job_vertex_deployment',
    default_args=default_args,
    description='A DAG to create, run, and delete a Google Batch job',
    schedule_interval=None,  # Change as required
)


get_latest_image_tag_task = PythonOperator(
    task_id='get_latest_image_tag',
    python_callable=get_latest_image_tag,
    dag=dag,
)

create_batch_job_task = PythonOperator(
    task_id='create_batch_job',
    python_callable=create_batch_job,
    provide_context=True,
    dag=dag,
)

monitor_batch_job_task = BatchJobSensor(
    task_id='monitor_batch_job',
    poke_interval=60,  # Check every 60 seconds
    timeout=3600,  # Timeout after 1 hour
    dag=dag,
)

submit_batch_job_task = PythonOperator(
    task_id='submit_batch_job',
    python_callable=submit_batch_job,
    provide_context=True,
    dag=dag,
)

delete_batch_job_task = BashOperator(
    task_id='delete_batch_job',
    bash_command=f'gcloud batch jobs delete ml-training-job --location={REGION} --quiet',
    dag=dag,
)

# Vertex AI tasks for model deployment
upload_model = UploadModelOperator(
    task_id="upload_model",
    project_id=PROJECT_ID,
    region=REGION,
    model={
        "display_name": "airline_model",
        "artifact_uri": f"gs://{MODEL_BUCKET_NAME}/{MODEL_PATH}/{MODEL_FILE}",
        "container_spec": {
            "image_uri": "gcr.io/deeplearning-platform-release/tf2-cpu.2-3:latest"
        },
    },
    dag=dag,
)

create_endpoint = CreateEndpointOperator(
    task_id="create_endpoint",
    project_id=PROJECT_ID,
    region=REGION,
    endpoint={"display_name": "airline_endpoint"},
    dag=dag,
)

deploy_model = DeployModelOperator(
    task_id="deploy_model",
    project_id=PROJECT_ID,
    region=REGION,
    traffic_split={"0": 100},  # Route 100% of traffic to the new model
    endpoint_id=create_endpoint.output["endpoint_id"],
    deployed_model={
        "display_name": "airline_model_deployed",
        "dedicated_resources": {
            "machine_spec": {"machine_type": "n1-standard-4"},
            "min_replica_count": 1,
            "max_replica_count": 1,
        },
    },
    dag=dag,
)

# Define success and failure email tasks
send_success_email = PythonOperator(
    task_id='send_success_email',
    python_callable=send_email,
    op_args=['success'],
    trigger_rule='all_success',
    dag=dag,
)

send_failure_email = PythonOperator(
    task_id='send_failure_email',
    python_callable=send_email,
    op_args=['failure'],
    trigger_rule='one_failed',
    dag=dag,
)

# Final dummy task to ensure all previous tasks are complete before sending email
end_task = DummyOperator(
    task_id='end',
    dag=dag,
)


"""
This section of the DAG defines the dependencies and the flow of tasks. 

Task Definitions:
1. get_latest_image_tag_task: Retrieves the latest Docker image tag for the batch job.
2. create_batch_job_task: Creates the configuration for the Google Batch job.
3. submit_batch_job_task: Submits the batch job to Google Cloud Batch for execution.
4. monitor_batch_job_task: Monitors the batch job until it completes successfully or fails.
5. delete_batch_job_task: Deletes the batch job configuration after monitoring is complete, whether successful or not. It is dependent on the completion of monitor_batch_job_task.
6. upload_model: Uploads the trained model to Vertex AI for deployment. It starts after the monitor_batch_job_task completes successfully.
7. create_endpoint: Creates an endpoint in Vertex AI for the model. It is dependent on the completion of the upload_model task.
8. deploy_model: Deploys the uploaded model to the created endpoint in Vertex AI. It follows the completion of the create_endpoint task.
9. end_task: A dummy task that signifies the end of the primary workflow. Both deploy_model and monitor_batch_job_task converge here.
10. send_success_email: Sends a success email notification. It is triggered if the end_task completes successfully.
11. send_failure_email: Sends a failure email notification. It is triggered if the end_task fails.

Dependencies:
- get_latest_image_tag_task >> create_batch_job_task >> submit_batch_job_task >> monitor_batch_job_task
- monitor_batch_job_task >> delete_batch_job_task
- monitor_batch_job_task >> upload_model >> create_endpoint >> deploy_model >> end_task
- end_task >> send_success_email
- end_task >> send_failure_email
"""

# Define task dependencies
(
    get_latest_image_tag_task
    >> create_batch_job_task
    >> submit_batch_job_task
    >> monitor_batch_job_task
)
monitor_batch_job_task >> delete_batch_job_task
monitor_batch_job_task >> upload_model >> create_endpoint >> deploy_model >> end_task
end_task >> send_success_email
end_task >> send_failure_email
