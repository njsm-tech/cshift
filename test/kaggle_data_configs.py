import os

import pytest 

from cshift.core.dataset import Dataset

KGL_DATASETS_DIR = 'datasets/kaggle'
WIND_PWR_PATH = os.path.join(KGL_DATASETS_DIR, 
        'wind_power/wind_power.parquet')
VACC_PATH = os.path.join(KGL_DATASETS_DIR, 
        'country_vaccinations/country_vaccinations.parquet')
DROUGHT_DIR = os.path.join(KGL_DATASETS_DIR,
        'drought/combined')
DROUGHT_PART_COLS = ['year']

@pytest.fixture(scope='package')
def wind_power():
    ds = Dataset.read(WIND_PWR_PATH)
    return ds.split(field_ord_split={'Date': .5})
