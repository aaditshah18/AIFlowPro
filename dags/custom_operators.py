from airflow.sensors.base import BaseSensorOperator
from google.cloud import batch_v1
from google.api_core.exceptions import GoogleAPICallError, RetryError
import time


class ExtendedBatchJobSensor(BaseSensorOperator):
    def __init__(self, job_name, project_id, location, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.job_name = job_name
        self.project_id = project_id
        self.location = location

    def poke(self, context):
        client = batch_v1.BatchServiceClient()
        job_name = (
            f"projects/{self.project_id}/locations/{self.location}/jobs/{self.job_name}"
        )

        try:
            job = client.get_job(name=job_name)
            self.log.info(f"Job status: {job.status.state}")
            if job.status.state == batch_v1.JobStatus.State.SUCCEEDED:
                return True
            elif job.status.state in (
                batch_v1.JobStatus.State.FAILED,
                batch_v1.JobStatus.State.DELETION_IN_PROGRESS,
            ):
                raise ValueError(
                    f"Batch job {self.job_name} failed or is being deleted."
                )
            return False
        except (GoogleAPICallError, RetryError) as e:
            self.log.error(f"Error occurred while checking job status: {e}")
            time.sleep(30)  # Adding a delay before retrying
            return False
