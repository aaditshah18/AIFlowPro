from google.cloud import pubsub_v1
from google.oauth2 import service_account
import json
from dotenv import load_dotenv
import os

load_dotenv()

credentials_dict = json.loads(os.environ.get('SERVICE_ACCOUNT_JSON'))

credentials_info = json.loads(credentials_dict)
credentials = service_account.Credentials.from_service_account_info(credentials_info)

subscriber = pubsub_v1.SubscriberClient(credentials=credentials)
subscription_path = subscriber.subscription_path('mlops-final-lab', 'model-subscription')

def callback(message):
    print(f"Received message: {message}")
    message.ack()
    
    data = json.loads(message.data.decode('utf-8'))
    object_name = data['name']
    
    if object_name.startswith("models/") and object_name.endswith("model.pkl"):
        print("Detected replacement of 'model.pkl' in the specified folder.")

future = subscriber.subscribe(subscription_path, callback)

try:
    future.result()
except KeyboardInterrupt:
    future.cancel()
