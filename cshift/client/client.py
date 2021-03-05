from functools import partialmethod

from .client_config import ClientConfig

class Client:
    def __init__(self, client_config: ClientConfig):
        self.client_config = client_config

    def register_dataset(self,
                         features=None,
                         labels=None,
                         name=None,
                         tags=None,
                         **kwargs):
        pass

    register_training_set = partialmethod(register_dataset, name='training')
    register_validation_set = partialmethod(register_dataset, name='validation')
    register_test_set = partialmethod(register_dataset, name='test')

    def compare_datasets(self, *args, **kwargs):
        pass