import smtplib
from email.mime.text import MIMEText
import logging
from airflow.models import Variable, Connection
from google.oauth2 import service_account
from googleapiclient.discovery import build
from airflow import settings
import json
import subprocess
import os
import pandas as pd
import numpy as np

import logging
import calendar


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



def print_preorder(path, indent=0):
    # Print the current directory or file
    print(' ' * indent + os.path.basename(path))
    
    # If the current path is a directory, recursively traverse its contents
    if os.path.isdir(path):
        for item in os.listdir(path):
            item_path = os.path.join(path, item)
            print_preorder(item_path, indent + 2)

# Specify the directory to start from
start_path = '/path/to/start/directory'


def download_code_from_gcs(**kwargs):   
    try:
        # Data Loading
        logging.info("Loading data from GCS")
        df1 = pd.read_csv('gs://us-central1-mlops-composer-c43be234-bucket/data/2015.csv').sample(frac=0.1, random_state=42)
        df2 = pd.read_csv('gs://us-central1-mlops-composer-c43be234-bucket/data/2016.csv').sample(frac=0.1, random_state=42)
        logging.info("Data loaded successfully")

        # Combine the datasets
        df = pd.concat([df1, df2], ignore_index=True)
        logging.info("Data combined successfully")

        # Renaming Airlines
        logging.info("Renaming airlines")
        df['OP_CARRIER'].replace(
            {
                'UA': 'United Airlines',
                'AS': 'Alaska Airlines',
                '9E': 'Endeavor Air',
                'B6': 'JetBlue Airways',
                'EV': 'ExpressJet',
                'F9': 'Frontier Airlines',
                'G4': 'Allegiant Air',
                'HA': 'Hawaiian Airlines',
                'MQ': 'Envoy Air',
                'NK': 'Spirit Airlines',
                'OH': 'PSA Airlines',
                'OO': 'SkyWest Airlines',
                'VX': 'Virgin America',
                'WN': 'Southwest Airlines',
                'YV': 'Mesa Airline',
                'YX': 'Republic Airways',
                'AA': 'American Airlines',
                'DL': 'Delta Airlines',
            },
            inplace=True,
        )

        # Dropping Columns
        logging.info("Dropping unnecessary columns")
        df = df.drop(["Unnamed: 27"], axis=1)

        # Handling Cancelled Flights
        logging.info("Handling cancelled flights")
        df = df[df['CANCELLED'] == 0]
        df = df.drop(['CANCELLED'], axis=1)

        # Handling Cancellation Codes
        logging.info("Dropping cancellation codes")
        df = df.drop(["CANCELLATION_CODE"], axis=1)

        # Dropping DIVERTED column
        logging.info("Dropping diverted column")
        df = df.drop(['DIVERTED'], axis=1)

        # Dropping Delay Reason Columns
        logging.info("Dropping delay reason columns")
        df = df.drop(
            [
                'CARRIER_DELAY',
                'WEATHER_DELAY',
                'NAS_DELAY',
                'SECURITY_DELAY',
                'LATE_AIRCRAFT_DELAY',
            ],
            axis=1,
        )

        # Dropping Flight Number
        logging.info("Dropping flight number")
        df = df.drop(['OP_CARRIER_FL_NUM'], axis=1)

        # Dropping Time Columns
        logging.info("Dropping time columns")
        df.drop(columns=['DEP_TIME', 'ARR_TIME'], inplace=True)

        # Handling Missing Values
        logging.info("Handling missing values")
        df["DEP_DELAY"] = df["DEP_DELAY"].fillna(0)
        df['TAXI_IN'].fillna((df['TAXI_IN'].mean()), inplace=True)
        df = df.dropna()

        # Binning Time Columns
        logging.info("Binning time columns")
        time_columns = ['CRS_DEP_TIME', 'WHEELS_OFF', 'WHEELS_ON', 'CRS_ARR_TIME']
        for col in time_columns:
            df[col] = np.ceil(df[col] / 600).astype(int)

        # Extracting Date Information
        logging.info("Extracting date information")
        df['DAY'] = pd.DatetimeIndex(df['FL_DATE']).day
        df['MONTH'] = pd.DatetimeIndex(df['FL_DATE']).month

        df['MONTH_AB'] = df['MONTH'].apply(lambda x: calendar.month_abbr[x])

        # Binary Classification
        logging.info("Applying binary classification")
        df['FLIGHT_STATUS'] = df['ARR_DELAY'].apply(lambda x: 0 if x < 0 else 1)

        # Convert FL_DATE to datetime and extract weekday
        logging.info("Converting FL_DATE to datetime and extracting weekday")
        df['FL_DATE'] = pd.to_datetime(df['FL_DATE'])
        df['WEEKDAY'] = df['FL_DATE'].dt.dayofweek

        # Drop unnecessary columns
        logging.info("Dropping unnecessary columns")
        df = df.drop(columns=['FL_DATE', 'MONTH_AB', 'ARR_DELAY'])

        # Saving the Cleaned Data
        logging.info("Saving cleaned data to cleaned.csv")
        df.to_csv('cleaned.csv', index=False)
        logging.info("Cleaned data saved successfully")

    except Exception as e:
        logging.error(f"Error occurred: {e}", exc_info=True)
        raise


def preprocess_data():
    logging.info("Running preprocessing script")

    # Ensure the directory exists
    script_dir = "/home/airflow/Desktop/modelling-mlops"
    os.makedirs(script_dir, exist_ok=True)

    script_path = os.path.join(script_dir, "preprocessing-cleaning.py")
    log_path = os.path.join(script_dir, "preprocessing-cleaning.log")

    python_command = f"python3 {script_path} > {log_path} 2>&1"

    try:
        subprocess.run(python_command, shell=True, check=True)
        logging.info("Preprocessing completed successfully")
    except subprocess.CalledProcessError as e:
        logging.error(f"Error in preprocessing script: {e}")
        raise


def train_model(**kwargs):
    username = kwargs['ti'].xcom_pull(task_ids='get_username')
    script_path = f"/home/{username}/Desktop/modelling-mlops/xgboost-model-v2.py"
    python_command = f"python3 {script_path}"
    subprocess.run(python_command, shell=True, check=True)





    
