import pytest

from typing import List

import os

from google.protobuf.json_format import MessageToJson
import pandas as pd

from cshift.client.client_config import ClientConfig
from cshift.client.client_dataset import ClientDataset
from cshift.core.dataset import Dataset
from cshift.proto import cshift_pb2 as pb2

from .random_data_configs import normal_unsep_ds

from conftest import client_config, DATASET_NAME, DATASET_VERSION, DATASET_TAGS

def test_concatenate(normal_unsep_ds: List[Dataset]):
    concatenated = Dataset.concatenate(*normal_unsep_ds)
    concatenated_df: pd.DataFrame = pd.concat([ds.df for ds in normal_unsep_ds], axis=0)
    assert concatenated.df.equals(concatenated_df)

def test_from_array(dataset: Dataset):
    ds = Dataset.from_array(
        dataset.df.values,
        dataset.df.columns.values.tolist())
    assert ds.df.equals(dataset.df)

def test_from_spec(dataset: Dataset, dataset_spec: pb2.DatasetSpec, client_config: ClientConfig):
    # register dataset spec
    client_dataset = ClientDataset(
        config=client_config, data=dataset.df, name=DATASET_NAME,
        tags=DATASET_TAGS, version=DATASET_VERSION)
    resp = client_dataset.register()
    assert resp.status_code == 200
    new_dataset = Dataset.from_spec(dataset_spec)
    assert new_dataset.df.equals(dataset.df)

def test_io(dataset: Dataset):
    path = './ds.tmp'
    dataset.write(path)
    read = dataset.read(path)
    try:
        assert dataset.df.equals(read.df)
    finally:
        os.remove(path)
