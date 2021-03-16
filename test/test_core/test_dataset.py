import pytest

from typing import List

import os

import pandas as pd

from cshift.client.client_config import ClientConfig
from cshift.client_service_common import config as csc_config
from cshift.core.dataset import Dataset
from cshift.dao.artifact import Artifact
from cshift.dao.artifact_gcs_path import ArtifactGcsPath
from cshift.proto import cshift_pb2 as pb2

from conftest import client_config

from random_data_configs import (
    normal_unsep_ds,
)

DATASET_NAME = 'test=dataset'
DATASET_TAGS = ['test']
DATASET_VERSION = '1'

@pytest.fixture(scope='module')
def dataset() -> Dataset:
    return normal_unsep_ds[0]

@pytest.fixture(scope='module')
def dataset_gcs_path(dataset: Dataset, client_config: ClientConfig) -> ArtifactGcsPath:
    return ArtifactGcsPath(
        bucket=csc_config.DATASETS_BUCKET,
        username=client_config.username,
        project=client_config.project,
        artifact_type=pb2.ArtifactType.DATASET,
        artifact_name=DATASET_NAME,
        artifact_version=DATASET_VERSION)

@pytest.fixture(scope='module')
def dataset_artifact_spec(dataset_gcs_path: ArtifactGcsPath) -> pb2.ArtifactSpec:
    return pb2.ArtifactSpec(
        name=DATASET_NAME,
        version=DATASET_VERSION,
        artifact_type=dataset_gcs_path.artifact_type,
        artifact_name=dataset_gcs_path.artifact_name,
        artifact_version=dataset_gcs_path.artifact_version)

@pytest.fixture(scope='module')
def dataset_artifact(dataset_artifact_spec: pb2.ArtifactSpec) -> Artifact:
    return Artifact(spec=dataset_artifact_spec)

@pytest.fixture(scope='module')
def dataset_spec(dataset_artifact_spec: pb2.ArtifactSpec) -> pb2.DatasetSpec:
    return pb2.DatasetSpec(
        name=DATASET_NAME,
        version=DATASET_VERSION,
        tags=DATASET_TAGS,
        artifact_spec=dataset_artifact_spec)

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
