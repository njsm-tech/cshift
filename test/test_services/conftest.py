from typing import Dict, List

import pytest

from flask import Flask
import pandas as pd
from sklearn.preprocessing import StandardScaler

from cshift.client.client_config import ClientConfig
from cshift.client.client_dataset import ClientDataset
from cshift.client.client_model import ClientModel
from cshift.client.client_comparison_pipeline import ClientComparisonPipeline
from cshift.services.compute_service import compute_service
from cshift.services.main_service import main_service

FEATURES_PATH = '../datasets/finance/stocks_features'
SCOPE = 'package'

config = ClientConfig(
    username='test-user',
    api_key='test-api-key',
    path='/Users/Nick/.cshift/cshift_config.json'
)

@pytest.fixture(scope=SCOPE)
def compute_service_app() -> Flask:
    yield compute_service.app

@pytest.fixture(scope=SCOPE)
def main_service_app() -> Flask:
    yield main_service.app

@pytest.fixture(scope=SCOPE)
def compute_service_client(compute_service_app: Flask):
    return compute_service_app.test_client()

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
def datasets(scaled_features: Dict[str, pd.DataFrame]) -> Dict[str, ClientDataset]:
    datasets = {}
    for (key, df) in scaled_features.items():
        datasets[key] = ClientDataset(data=df, name=key, tags=[key])
    return datasets

@pytest.fixture(scope=SCOPE)
def model(datasets: Dict[str, ClientDataset]) -> ClientModel:
    return ClientModel(
        name='test-finance-model',
        training_set=datasets['train'])

@pytest.fixture(scope=SCOPE)
def comparison_pipelines(datasets: Dict[str, ClientDataset]) -> List[ClientComparisonPipeline]:
    pipelines = []
    index_fields = ['dt', 'ticker_cat']
    groupby_fields = ['ticker_cat']
    comparison_types = ['ks', 'lr', 'summary_stats']
    pipelines.append(ClientComparisonPipeline(
        datasets=[datasets['train'], datasets['val']],
        index_fields=index_fields,
        groupby_fields=groupby_fields,
        comparison_types=comparison_types
    ))
    pipelines.append(ClientComparisonPipeline(
        datasets=[datasets['train'], datasets['test']],
        index_fields=index_fields,
        groupby_fields=groupby_fields,
        comparison_types=comparison_types
    ))
    return pipelines