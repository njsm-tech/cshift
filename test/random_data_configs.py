from cshift import pb2
from cshift.column import Column
from cshift.dataset import Dataset

NFEAT = 5
SIZE = 10000
RAND_STATE = 42

def gen_normal_ds(ncols=None, locs=None, scales=None, size=None) -> Dataset:
    cols = []
    for i in range(NFEAT):
        kwargs = {'loc': locs[i], 'scale': scales[i]}
        cols.append(gen_col(dist_name='norm', size=size, kwargs=kwargs))
    return Dataset.from_columns(cols)

def gen_col(dist_name=None, size=None, kwargs=None) -> Column:
    rspec = RandomColumnSpec(
        dist_name=dist_name,
        size=size,
        kwargs=kwargs)
    cspec = ColumnSpec(
        name='col' + str(i),
        random_column_spec=rspec)
    return Column.generate_from_spec(cspec)

def unsep_normal_ds1() -> Dataset:
    locs = [-2., -1., 0., 1., 2.]
    scales = [1., 1., 0.5, 0.5, 0.5]
    return gen_normal_ds(ncols=NFEAT, locs=locs, scales=scales, size=SIZE)

def sep_normal_ds1() -> Dataset:
    locs = [-2., -1., 0., 1., 2.]
    scales = [1., 1., 0.5, 0.5, 0.5]
    ds1 = gen_normal_ds(ncols=NFEAT, locs=locs, scales=scales, size=SIZE/2)
    locs = [-1., -1., 0., 0.5, 1.]
    scales = [1., 0.5, 2., 1., 0.5]
    ds2 = gen_normal_ds(ncols=NFEAT, locs=locs, scales=scales, size=SIZE/2)
    return Dataset.concatenate(ds1, ds2)    

