from airflow import DAG
from airflow.providers.google.cloud.operators.vertex_ai.model_service import (
    UploadModelOperator,
)
from airflow.providers.google.cloud.operators.vertex_ai.endpoint_service import (
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

PROJECT_ID = Variable.get("PROJECT_ID")
ZONE = Variable.get("ZONE")
REGION = Variable.get("REGION")
MODEL_BUCKET_NAME = Variable.get("MODEL_BUCKET_NAME")
MODEL_PATH = Variable.get("MODEL_PATH")
MODEL_FILE = Variable.get("MODEL_FILE")

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

# Define the task dependencies
upload_model >> create_endpoint >> deploy_model
