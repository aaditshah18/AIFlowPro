from airflow.sensors.base_sensor_operator import BaseSensorOperator
from google.api_core.client_options import ClientOptions
from google.cloud import batch_v1
from airflow.models import Variable

PROJECT_ID = Variable.get("PROJECT_ID")
REGION = Variable.get("REGION")


class BatchJobSensor(BaseSensorOperator):
    def poke(self, context):
        client_options = ClientOptions(api_endpoint="https://batch.googleapis.com/")
        client = batch_v1.BatchServiceClient(client_options=client_options)
        job_name = f"projects/{PROJECT_ID}/locations/{REGION}/jobs/ml-training-job"

        job = client.get_job(name=job_name)

        state = job.status.state
        if state in [
            batch_v1.JobStatus.State.SUCCEEDED,
            batch_v1.JobStatus.State.FAILED,
            batch_v1.JobStatus.State.DELETION_IN_PROGRESS,
        ]:
            return True
        return False
