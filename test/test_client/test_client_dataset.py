import pytest

import numpy as np
import pandas as pd

from conftest import client_config

from cshift.client.client_config import ClientConfig
from cshift.client.client_dataset import ClientDataset

@pytest.fixture(scope='module')
def data() -> pd.DataFrame:
    numeric_data = np.random.random(size=(100, 3))
    cat_data = np.arange(4)\
        .reshape(-1, 1)\
        .repeat(25, axis=1)\
        .reshape(-1, 1)\
        .repeat(3, axis=1)
    data = np.hstack([numeric_data, cat_data])
    return pd.DataFrame(data, columns=list('abcdef'))

@pytest.fixture(scope='module')
def client_dataset(
        client_config: ClientConfig,
        data: pd.DataFrame) -> ClientDataset:
    return ClientDataset(
        config=client_config,
        data=data,
        feature_cols=list('abcdef'),
        label_cols=None,
        name='test-dataset',
        tags=['test'])

def test_dataset_attributes(
        client_dataset: ClientDataset,
        data: pd.DataFrame):
    assert client_dataset.is_data_literal
    assert not client_dataset.is_data_ref
    assert data.equals(client_dataset.data)

def test_register(client_dataset: ClientDataset):
    res = client_dataset.register()
    js = res.json()
    print(js)
    assert res.status_code == 200