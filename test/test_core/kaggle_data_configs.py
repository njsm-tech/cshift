import os

import pytest 
from conftest import DATASETS_DIR

from cshift.core.dataset import Dataset

KGL_DATASETS_DIR = os.path.join(
    DATASETS_DIR,
    'kaggle')
WIND_PWR_PATH = os.path.join(
    KGL_DATASETS_DIR,
    'wind_power/wind_power.parquet')
VACC_PATH = os.path.join(
    KGL_DATASETS_DIR,
    'country_vaccinations/country_vaccinations.parquet')
DROUGHT_DIR = os.path.join(
    KGL_DATASETS_DIR,
    'drought/combined')
DROUGHT_DIR_SMALL = os.path.join(
    KGL_DATASETS_DIR,
    'drought/small')

@pytest.fixture(scope='package')
def wind_power():
    ds = Dataset.read(WIND_PWR_PATH)
    return ds.split(field_ord_split={'Date': .5})

@pytest.fixture(scope='package')
def drought():
    ds = Dataset.read(DROUGHT_DIR_SMALL)
    return ds.split(field_ord_split={'year': .5})

#@pytest.fixture(scope='package')
#def country_vaccinations():
#    ds = Dataset.read(VACC_PATH)
