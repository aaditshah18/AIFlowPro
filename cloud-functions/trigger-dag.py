import base64
import json
import os
import google.auth
from google.auth.transport.requests import Request
from google.oauth2 import id_token
import requests
import time
import functions_framework
import logging

# Set up logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@functions_framework.cloud_event
def trigger_dag(cloud_event):
    # Set up parameters for triggering the Airflow DAG
    logger.info("Process started with cloud event")
    dag_id = os.getenv('DAG_ID')
    execution_date = None  # Setting this to None so Airflow to uses the current time

    project_id = os.getenv('PROJECT_ID')
    location = os.getenv('LOCATION')
    composer_environment = os.getenv('COMPOSER_ENVIRONMENT')
    url = f"https://composer.googleapis.com/v1/projects/{project_id}/locations/{location}/environments/{composer_environment}/dagRuns"

    # Use default credentials to get an auth token
    credentials, _ = google.auth.default()
    auth_request = Request()
    credentials.refresh(auth_request)
    auth_token = id_token.fetch_id_token(auth_request, url)

    headers = {
        'Authorization': f'Bearer {auth_token}',
        'Content-Type': 'application/json',
    }

    data = {
        'dag_id': dag_id,
        'execution_date': execution_date,
    }

    # Sleep for 2.5 minutes (150 seconds)
    time.sleep(150)

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        logger.info('DAG triggered successfully')
    else:
        logger.error(f'Failed to trigger DAG: {response.text}')
