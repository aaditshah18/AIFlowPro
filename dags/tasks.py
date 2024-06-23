import smtplib
from email.mime.text import MIMEText
import logging
from airflow.models import Variable, Connection
from google.oauth2 import service_account
from googleapiclient.discovery import build
from airflow import settings
import json
import subprocess


PROJECT_ID = Variable.get("PROJECT_ID")
ZONE = Variable.get("ZONE")  # us-central1-a
REGION = Variable.get("REGION").strip()  # us-central1
INSTANCE_NAME = Variable.get("GCE_INSTANCE_NAME")
GCP_SERVICE_ACCOUNT_KEY_JSON = Variable.get("GCP_SERVICE_ACCOUNT_KEY_JSON")


def send_email(status):
    sender_email = Variable.get('EMAIL_USER')
    receiver_email = Variable.get("RECEIVER_EMAIL")
    password = Variable.get('EMAIL_PASSWORD')

    if status == "success":
        subject = "Airflow DAG Succeeded"
        body = "Hello, your DAG has completed successfully."
    else:
        subject = "Airflow DAG Failed"
        body = "Hello, your DAG has failed. Please check the logs for more details."

    # Create the email headers and content
    email_message = MIMEText(body)
    email_message['Subject'] = subject
    email_message['From'] = sender_email
    email_message['To'] = receiver_email

    try:
        # Set up the SMTP server
        server = smtplib.SMTP('smtp.gmail.com', 587)  # Using Gmail's SMTP server
        server.starttls()  # Secure the connection
        server.login(sender_email, password)
        server.sendmail(sender_email, receiver_email, email_message.as_string())
        logging.info("Email sent successfully!")
    except Exception as e:
        logging.error(f"Error sending email: {e}")
    finally:
        server.quit()


def get_external_ip(ti):
    # Ensure the environment variable is set
    if not GCP_SERVICE_ACCOUNT_KEY_JSON:
        raise ValueError(
            "The environment variable GCP_SERVICE_ACCOUNT_KEY_JSON is not set."
        )

    credentials = service_account.Credentials.from_service_account_info(
        json.loads(GCP_SERVICE_ACCOUNT_KEY_JSON)
    )
    service = build('compute', 'v1', credentials=credentials)
    request = service.instances().get(
        project=PROJECT_ID, zone=ZONE, instance=INSTANCE_NAME
    )
    response = request.execute()
    network_interface = response['networkInterfaces'][0]
    access_config = network_interface['accessConfigs'][0]
    external_ip = access_config['natIP']
    ssh_username = ti.xcom_pull(task_ids='get_username')

    # Create or update the SSH connection in Airflow
    conn_id = 'dynamic_ssh_connection'
    conn = Connection(
        conn_id=conn_id,
        conn_type='ssh',
        host=external_ip,
        schema='',
        login=ssh_username,
        password='',
        port=22,
        extra=json.dumps(
            {"key_file": f"/home/{ssh_username}/.ssh/id_rsa", "timeout": "10"}
        ),
    )
    session = settings.Session()
    if not session.query(Connection).filter(Connection.conn_id == conn_id).first():
        session.add(conn)
    else:
        existing_conn = (
            session.query(Connection).filter(Connection.conn_id == conn_id).first()
        )
        existing_conn.host = conn.host
        existing_conn.login = conn.login
        existing_conn.extra = conn.extra
    session.commit()


def create_ssh_connection(ti):
    external_ip = ti.xcom_pull(task_ids='get_external_ip')
    ssh_username = ti.xcom_pull(task_ids='get_username')

    conn = Connection(
        conn_id='dynamic_ssh_connection',
        conn_type='ssh',
        host=external_ip,
        schema='',
        login=ssh_username,
        password='',
        port=22,
        extra=json.dumps(
            {"key_file": f"/home/{ssh_username}/.ssh/id_rsa", "conn_timeout": "10"}
        ),
    )

    session = settings.Session()
    existing_conn = (
        session.query(Connection)
        .filter(Connection.conn_id == 'dynamic_ssh_connection')
        .first()
    )

    if existing_conn:
        session.delete(existing_conn)
        session.commit()

    session.add(conn)
    session.commit()


def download_code_from_gcs(**kwargs):
    username = kwargs['ti'].xcom_pull(task_ids='get_username')
    destination_path = f"/home/{username}/Desktop/modelling-mlops"

    # Create the destination directory if it doesn't exist
    subprocess.run(['mkdir', '-p', destination_path], check=True)

    # Download the files from GCS
    gsutil_command = f"gsutil -m cp -r gs://modelling-mlops/* {destination_path}"
    subprocess.run(gsutil_command, shell=True, check=True)


def preprocess_data():
    try:
        logging.info("Running preprocessing script")
        result = subprocess.run(
            "python3 /home/airflow/Desktop/modelling-mlops/preprocessing-cleaning.py > /home/airflow/Desktop/modelling-mlops/preprocessing-cleaning.log 2>&1",
            shell=True,
            check=True,
            capture_output=True,
            text=True,
        )
        logging.info("Preprocessing script output: %s", result.stdout)
    except subprocess.CalledProcessError as e:
        logging.error("Error in preprocessing script: %s", e.stderr)
        raise


def train_model(**kwargs):
    username = kwargs['ti'].xcom_pull(task_ids='get_username')
    script_path = f"/home/{username}/Desktop/modelling-mlops/xgboost-model-v2.py"
    python_command = f"python3 {script_path}"
    subprocess.run(python_command, shell=True, check=True)
