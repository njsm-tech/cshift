import pytest

from cshift.core import cshift_pb2 as pb2
from cshift.core.column import Column
from cshift.core.dataset import Dataset

NFEAT = 5
SIZE = 10000
RAND_STATE = 42

def gen_normal_ds(ncols=None, locs=None, scales=None, size=None) -> Dataset:
    cols = []
    for i in range(NFEAT):
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

@pytest.fixture(scope='package')
def normal_unsep_ds() -> Dataset:
    locs = [-2., -1., 0., 1., 2.]
    scales = [.1, .1, .1, .1, .1]
    ds1 = gen_normal_ds(ncols=NFEAT, locs=locs, scales=scales, size=SIZE)
    ds2 = gen_normal_ds(ncols=NFEAT, locs=locs, scales=scales, size=SIZE)
    return [ds1, ds2]

@pytest.fixture(scope='package')
def normal_sep_ds() -> Dataset:
    locs = [-2., -1., 0., 1., 2.]
    scales = [.1, .1, .05, .05, .05]
    ds1 = gen_normal_ds(ncols=NFEAT, locs=locs, scales=scales, size=SIZE)
    locs = [-1., -1., 0., .5, 1.]
    scales = [.1, .1, .05, .05, .05]
    ds2 = gen_normal_ds(ncols=NFEAT, locs=locs, scales=scales, size=SIZE)
    return [ds1, ds2]

