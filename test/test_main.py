from typing import List

import numpy as np
import scipy.stats as ss

from cshift.core.dataset import Dataset

NFEAT = 5
SIZE = 1000
STATE = 42

def test_main():
    colnames: List[str] = ['feat' + str(i) for i in range(NFEAT)]
    ds_sep: Dataset = gen_separated_data(colnames)
    ds_unsep: Dataset = gen_unseparated_data(colnames)

def gen_unseparated_data(colnames: List[str]) -> Dataset:
    locs = gen_data(size=NFEAT)
    scales = np.abs(gen_data(size=NFEAT))
    arr = gen_data(loc=locs, scale=scale, size=SIZE)
    return Dataset.from_array(arr=arr, colnames=colnames)

def gen_separated_data(colnames: List[str]) -> Dataset:
    locs1 = gen_data(size=NFEAT)
    scales1 = np.abs(gen_data(size=NFEAT))
    arr1 = gen_data(loc=locs1, scale=scales1, size=SIZE/2)

    locs2 = gen_data(size=NFEAT)
    scales2 = np.abs(gen_data(size=NFEAT))
    arr2 = gen_data(loc=locs2, scale=scales2, size=SIZE/2)

    arr = np.concatenate([arr1, arr2], axis=0)
    return Dataset.from_array(arr=arr, colnames=colnames)

def gen_data(dist=ss.norm, loc=0., scale=1., size=1) -> Dataset:
    return dist.rvs(loc=loc, scale=scale, size=size)
