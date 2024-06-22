from airflow import DAG
from airflow.providers.google.cloud.operators.vertex_ai import (
    UploadModelOperator,
    CreateEndpointOperator,
    DeployModelOperator,
)
from airflow.utils.dates import days_ago
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

dag = DAG(
    'train_and_deploy_dag',
    default_args=default_args,
    description='Deploy the trained model to Vertex AI',
    schedule_interval=None,  # Triggered by the first DAG
)

PROJECT_ID = Variable.get("gcp_project_id")
ZONE = Variable.get("gcp_zone")
MODEL_BUCKET_NAME = 'your-model-bucket'
MODEL_PATH = 'path/to/model'
MODEL_FILE = 'model.pkl'

# Vertex AI tasks for model deployment
upload_model = UploadModelOperator(
    task_id="upload_model",
    model_display_name="airline_model",
    project_id=PROJECT_ID,
    location=ZONE,
    artifact_uri=f"gs://{MODEL_BUCKET_NAME}/{MODEL_PATH}/{MODEL_FILE}",
    serving_container_image_uri="gcr.io/deeplearning-platform-release/tf2-cpu.2-3:latest",
    dag=dag,
)

create_endpoint = CreateEndpointOperator(
    task_id="create_endpoint",
    endpoint_display_name="airline_endpoint",
    project_id=PROJECT_ID,
    location=ZONE,
    dag=dag,
)

deploy_model = DeployModelOperator(
    task_id="deploy_model",
    model_id="{{ task_instance.xcom_pull(task_ids='upload_model')['model'] }}",
    endpoint_id="{{ task_instance.xcom_pull(task_ids='create_endpoint')['endpoint'] }}",
    project_id=PROJECT_ID,
    location=ZONE,
    deployed_model_display_name="airline_model_deployed",
    dag=dag,
)

# Define the task dependencies
upload_model >> create_endpoint >> deploy_model
