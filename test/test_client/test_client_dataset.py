import pytest

import pandas as pd

from conftest import client_config, client_data, client_dataset

from cshift.client.client_dataset import ClientDataset

SCOPE = 'package'

def test_dataset_attributes(
        client_dataset: ClientDataset,
        client_data: pd.DataFrame):
    assert client_dataset.is_data_literal
    assert not client_dataset.is_data_ref
    assert client_data.equals(client_dataset.data)

def test_register(client_dataset: ClientDataset):
    res = client_dataset.register()
    assert res.status_code == 200