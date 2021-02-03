from typing import List

import os
import pytest

import numpy as np

from cshift.proto import cshift_pb2 as pb2
from cshift.core.column import Column
from cshift.core.dataset import Dataset

ROOT_DIR = '~/code/cshift'
GEN_DATASETS_DIR = os.path.join(ROOT_DIR, 'test/datasets/generated')
SCOPE = 'session'

NFEAT = 5
SIZE = 2000
RAND_STATE = 42
DAYS = 14

READ = True
WRITE = True

def gen_normal_ds(ncols=NFEAT, locs=None, scales=None, size=None) -> Dataset:
    cols = []
    for i in range(ncols):
        kwargs = {'loc': locs[i], 'scale': scales[i]}
        name = 'norm' + str(i)
        cols.append(gen_col(dist_name='norm', size=size, kwargs=kwargs, name=name))
    return Dataset.from_columns(cols)

def gen_time_col(size=None, days=None):
    time_arr = np.arange(days).reshape(-1, 1)
    time_arr = np.repeat(time_arr, size, axis=1).reshape(1, -1).T
    return Column(name='day', arr=time_arr)

def gen_ts_normal_ds(ncols=NFEAT, locs=None, scales=None, size=None, days=None) -> Dataset:
    cols = []
    for i in range(ncols):
        kwargs = {'loc': locs[i], 'scale': scales[i]}
        name = 'norm' + str(i)
        cols.append(gen_col(dist_name='norm', size=size, kwargs=kwargs, name=name))
    return Dataset.from_columns(cols)

def gen_col(dist_name=None, size=None, kwargs=None, name=None) -> Column:
    rspec = pb2.RandomColumnSpec(
        dist_name=dist_name,
        size=size,
        kwargs=kwargs)
    cspec = pb2.ColumnSpec(
        name=name,
        random_column_spec=rspec)
    return Column.generate_from_spec(cspec)

@pytest.fixture(scope=SCOPE)
def normal_unsep_ds() -> List[Dataset]:
    base_path = os.path.join(GEN_DATASETS_DIR, 'normal_unsep_ds')
    if READ:
        ds1 = Dataset.read(base_path + '1')
        ds2 = Dataset.read(base_path + '2')
    else:
        locs = [-2., -1., 0., 1., 2.]
        scales = [.1, .1, .1, .1, .1]
        ds1 = gen_normal_ds(ncols=NFEAT, locs=locs, scales=scales, size=SIZE)
        ds2 = gen_normal_ds(ncols=NFEAT, locs=locs, scales=scales, size=SIZE)
    if WRITE:
        ds1.write(base_path + '1')
        ds2.write(base_path + '2')
    return [ds1, ds2]

@pytest.fixture(scope=SCOPE)
def normal_sep_ds() -> List[Dataset]:
    base_path = os.path.join(GEN_DATASETS_DIR, 'normal_sep_ds')
    if READ:
        ds1 = Dataset.read(base_path + '1')
        ds2 = Dataset.read(base_path + '2')
    else:
        locs = [-2., -1., 0., 1., 2.]
        scales = [.1, .1, .05, .05, .05]
        ds1 = gen_normal_ds(ncols=NFEAT, locs=locs, scales=scales, size=SIZE)
        locs = [-1., -1., 0., .5, 1.]
        scales = [.1, .1, .05, .05, .05]
        ds2 = gen_normal_ds(ncols=NFEAT, locs=locs, scales=scales, size=SIZE)
    if WRITE:
        ds1.write(base_path + '1')
        ds2.write(base_path + '2')
    return [ds1, ds2]

@pytest.fixture(scope=SCOPE)
def ts_normal_unsep_ds() -> List[Dataset]:
    base_path = os.path.join(GEN_DATASETS_DIR, 'ts_normal_unsep_ds')
    if READ:
        ds1 = Dataset.read(base_path + '1')
        ds2 = Dataset.read(base_path + '2')
    else:
        locs = [-2., -1., 0., 1., 2.]
        scales = [.1, .1, .1, .1, .1]
        ds1 = gen_ts_normal_ds(ncols=NFEAT, locs=locs, scales=scales, size=SIZE, days=DAYS)
        ds2 = gen_ts_normal_ds(ncols=NFEAT, locs=locs, scales=scales, size=SIZE, days=DAYS)
    if WRITE:
        ds1.write(base_path + '1')
        ds2.write(base_path + '2')
    return [ds1, ds2]

@pytest.fixture(scope=SCOPE)
def normal_sep_ds() -> List[Dataset]:
    base_path = os.path.join(GEN_DATASETS_DIR, 'normal_sep_ds')
    if READ:
        ds1 = Dataset.read(base_path + '1')
        ds2 = Dataset.read(base_path + '2')
    else:
        locs = [-2., -1., 0., 1., 2.]
        scales = [.1, .1, .05, .05, .05]
        ds1 = gen_normal_ds(ncols=NFEAT, locs=locs, scales=scales, size=SIZE)
        locs = [-1., -1., 0., .5, 1.]
        scales = [.1, .1, .05, .05, .05]
        ds2 = gen_normal_ds(ncols=NFEAT, locs=locs, scales=scales, size=SIZE)
    if WRITE:
        ds1.write(base_path + '1')
        ds2.write(base_path + '2')
    return [ds1, ds2]

