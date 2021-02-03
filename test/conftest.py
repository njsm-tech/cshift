from typing import Dict, List

import os
import pytest

from flask import Flask
import numpy as np
import pandas as pd
from sklearn.preprocessing import StandardScaler

from cshift.client.client_config import ClientConfig
from cshift.client.client_dataset import ClientDataset
from cshift.client.client_model import ClientModel
from cshift.client.client_comparison_pipeline import ClientComparisonPipeline
from cshift.client_service_common import config as csc_config
from cshift.core.dataset import Dataset
from cshift.dao.artifact import Artifact
from cshift.dao.artifact_gcs_path import ArtifactGcsPath
from cshift.proto import cshift_pb2 as pb2
from cshift.services.compute_service import compute_service
from cshift.services.main_service import main_service

from test_core.random_data_configs import normal_unsep_ds

TEST_PROJECT = 'pytest'
TEST_USER = 'test-user'
TEST_API_KEY = 'test-api-key'

ROOT = '~/code/cshift'
TEST_ROOT = os.path.join(ROOT, 'test')
DATASETS_DIR = os.path.join(TEST_ROOT, 'datasets')
FEATURES_PATH = os.path.join(
    DATASETS_DIR,
    'finance/stocks_features')
SCOPE = 'session'

@pytest.fixture(scope=SCOPE)
def client_config():
    yield ClientConfig(
        username=TEST_USER,
        api_key=TEST_API_KEY,
        project=TEST_PROJECT
    )

@pytest.fixture(scope=SCOPE)
def main_service_app() -> Flask:
    yield main_service.app

@pytest.fixture(scope=SCOPE)
def main_service_client(main_service_app: Flask):
    return main_service_app.test_client()

@pytest.fixture(scope=SCOPE)
def features_df() -> pd.DataFrame:
    raw_df = pd.read_parquet(FEATURES_PATH)
    df = raw_df.dropna(how='any', axis=0)
    df = df.drop_duplicates(subset=['ticker_cat', 'dt'])
    df = df.sort_values(by=['dt', 'ticker_cat'])
    df = df.drop(columns=['dt', 'vw'])
    df = df.reset_index(drop=True)
    return df

@pytest.fixture(scope=SCOPE)
def train_val_test_split(features_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    l = len(features_df)
    train_idx, val_idx = int(l * .7), int(l * .85)
    train_df = features_df.loc[:train_idx, :]
    val_df = features_df.loc[train_idx:val_idx, :]
    test_df = features_df.loc[val_idx:, :]
    return {'train': train_df, 'val': val_df, 'test': test_df}

@pytest.fixture(scope=SCOPE)
def scaled_features(
        train_val_test_split: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
    numeric_cols = [
        'bookvaluepershare',
         'currentratio',
         'debttoequityratio',
         'dividendyield',
         'dividendsperbasiccommonshare',
         'e_bi_td_amargin',
         'profitmargin',
         'payoutratio',
         'pricetobookvalue',
         'pricetoearningsratio',
         'pricetosalesratio',
         'netincome_to_revenues_ratio',
         'roe',
         'time_of_day',
         'days_since_earnings_report',
         'days_until_earnings_report',
         'vw_slope2',
         'vw_slope5',
         'vw_slope10',
         'vw_slope20',
         'vw_slope50',
         'vw_slope200'
    ]
    train_df = train_val_test_split['train']
    val_df = train_val_test_split['val']
    test_df = train_val_test_split['test']
    scaler = StandardScaler()
    train_df[numeric_cols] = scaler.fit_transform(train_df[numeric_cols])
    val_df[numeric_cols] = scaler.transform(val_df[numeric_cols])
    test_df[numeric_cols] = scaler.transform((test_df[numeric_cols]))
    return {'train': train_df, 'val': val_df, 'test': test_df}

@pytest.fixture(scope=SCOPE)
def client_datasets(client_config: ClientConfig,
                    scaled_features: Dict[str, pd.DataFrame]) -> Dict[str, ClientDataset]:
    datasets = {}
    for (key, df) in scaled_features.items():
        datasets[key] = ClientDataset(
            config=client_config,
            data=df,
            name=key,
            tags=[key]
        )
    return datasets

@pytest.fixture(scope=SCOPE)
def client_model(client_config: ClientConfig,
                 client_datasets) -> ClientModel:
    return ClientModel(
        config=client_config,
        name='test-finance-model',
        training_set=client_datasets['train'])

@pytest.fixture(scope=SCOPE)
def client_comparison_pipelines(
        client_config: ClientConfig,
        client_datasets) -> List[ClientComparisonPipeline]:
    pipelines = []
    index_fields = ['dt', 'ticker_cat']
    groupby_fields = ['ticker_cat']
    comparison_types = ['ks', 'lr', 'summary_stats']
    pipelines.append(ClientComparisonPipeline(
        config=client_config,
        datasets=[client_datasets['train'], client_datasets['val']],
        index_fields=index_fields,
        groupby_fields=groupby_fields,
        comparison_types=comparison_types
    ))
    pipelines.append(ClientComparisonPipeline(
        config=client_config,
        datasets=[client_datasets['train'], client_datasets['test']],
        index_fields=index_fields,
        groupby_fields=groupby_fields,
        comparison_types=comparison_types
    ))
    return pipelines

@pytest.fixture(scope=SCOPE)
def client_data() -> pd.DataFrame:
    numeric_data = np.random.random(size=(100, 3))
    cat_data = np.arange(4)\
        .reshape(-1, 1)\
        .repeat(25, axis=1)\
        .reshape(-1, 1)\
        .repeat(3, axis=1)
    data = np.hstack([numeric_data, cat_data])
    return pd.DataFrame(data, columns=list('abcdef'))

@pytest.fixture(scope=SCOPE)
def client_dataset(
        client_config: ClientConfig,
        client_data: pd.DataFrame) -> ClientDataset:
    return ClientDataset(
        config=client_config,
        data=client_data,
        feature_cols=list('abcdef'),
        label_cols=None,
        name='test-client-dataset',
        tags=['test'])

@pytest.fixture(scope=SCOPE)
def client_comparison_pipeline(
        client_dataset: ClientDataset) -> ClientComparisonPipeline:
    return ClientComparisonPipeline(
        datasets=[client_dataset, client_dataset],
        groupby_fields=['a', 'b'],
        index_fields=[],
        comparison_types=None)

### test_core fixtures
DATASET_NAME = 'test-dataset-normal-unsep-ds'
DATASET_TAGS = ['test']
DATASET_VERSION = '1'

@pytest.fixture(scope=SCOPE)
def dataset(normal_unsep_ds: List[Dataset]) -> Dataset:
    return normal_unsep_ds[0]

@pytest.fixture(scope=SCOPE)
def dataset_gcs_path(dataset: Dataset, client_config: ClientConfig) -> ArtifactGcsPath:
    return ArtifactGcsPath(
        bucket=csc_config.DATASETS_BUCKET,
        username=client_config.username,
        project=client_config.project,
        artifact_type=pb2.ArtifactType.DATASET,
        artifact_name=DATASET_NAME,
        artifact_version=DATASET_VERSION)

@pytest.fixture(scope=SCOPE)
def dataset_artifact_spec(dataset_gcs_path: ArtifactGcsPath) -> pb2.ArtifactSpec:
    return pb2.ArtifactSpec(
        name=DATASET_NAME,
        version=DATASET_VERSION,
        artifact_type=dataset_gcs_path.artifact_type,
        gcs_path=dataset_gcs_path.to_message(),
        deserialized_type=pb2.ArtifactDeserializedType.PANDAS_DATAFRAME,
        serialization_format=pb2.ArtifactSerializationFormat.PARQUET)

@pytest.fixture(scope=SCOPE)
def dataset_artifact(dataset_artifact_spec: pb2.ArtifactSpec) -> Artifact:
    return Artifact(spec=dataset_artifact_spec)

@pytest.fixture(scope=SCOPE)
def dataset_spec(dataset_artifact_spec: pb2.ArtifactSpec) -> pb2.DatasetSpec:
    return pb2.DatasetSpec(
        name=DATASET_NAME,
        version=DATASET_VERSION,
        tags=DATASET_TAGS,
        artifact_spec=dataset_artifact_spec)