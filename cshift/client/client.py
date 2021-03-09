from typing import Dict

from functools import partialmethod
import requests

from cshift.client_service_common import api_paths
from .client_comparison import ClientComparison
from .client_config import ClientConfig
from .client_dataset import ClientDataset
from .client_model import ClientModel

class Client:
    def __init__(self, client_config: ClientConfig):
        self.client_config = client_config

    def register_dataset(self, dataset: ClientDataset):
        return requests.post(
            url=api_paths.REGISTER_DATASET,
            headers={'Content-Type': 'application/protobuf'},
            data=dataset.dataset_spec.SerializeToString()
        )

    register_training_set = partialmethod(register_dataset, name='training')
    register_validation_set = partialmethod(register_dataset, name='validation')
    register_test_set = partialmethod(register_dataset, name='test')

    def register_model(self, model: ClientModel):
        return requests.post(
            url=api_paths.REGISTER_MODEL,
            headers={'Content-Type': 'application/protobuf'},
            data=model.model_spec.SerializeToString()
        )

    def compare_datasets(self, comparison: ClientComparison):
        pass

