from dataclasses import dataclass

from google.cloud import datastore

from cshift.client_service_common import config as csc_config
from cshift.enums import JobStatus

datastore_client = datastore.Client(project=csc_config.PROJECT)

@dataclass
class Job:
    job_id: str = None,
    status: JobStatus = None

    @property
    def key(self):
        return datastore_client.key(csc_config.JOBS_KEY, self.job_id)

    def get_from_datastore(self):
        return datastore_client.get(self.key)

    def put_to_datastore(self, entity):
        datastore_client.put(entity)

    def mark_status(self, status: JobStatus):
        entity = self.get_from_datastore()
        entity['status'] = status.value
        self.put_to_datastore(entity)