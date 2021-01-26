from cshift.pb2 import ColumnSpec, DatasetSpec, RandomColumnSpec

NFEAT = 5
SIZE = 1000
STATE = 42

def normal_data(loc=0., scale=1.):
    return RandomColumnSpec(
        dist_name='norm',
        loc=0.,
        scale=1.,
        size=SIZE)
