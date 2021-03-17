import pytest

from typing import List

import os

import pandas as pd

from cshift.core.dataset import Dataset
from cshift.proto import cshift_pb2 as pb2

from .random_data_configs import normal_unsep_ds

def test_concatenate(normal_unsep_ds: List[Dataset]):
    concatenated = Dataset.concatenate(*normal_unsep_ds)
    concatenated_df: pd.DataFrame = pd.concat([ds.df for ds in normal_unsep_ds], axis=0)
    assert concatenated.df.equals(concatenated_df)

def test_from_array(dataset: Dataset):
    ds = Dataset.from_array(
        dataset.df.values,
        dataset.df.columns.names)
    assert ds.df.equals(dataset.df)

def test_from_spec(dataset: Dataset, dataset_spec: pb2.DatasetSpec):
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
