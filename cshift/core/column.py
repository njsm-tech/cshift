import numpy as np
import pandas as pd
import scipy.stats as ss

class Column:
    # TODO: add type? 
    def __init__(self, name, arr):
        self.name = name
        self.arr = arr

    @classmethod
    def from_series(cls, series: pd.Series):
        pass

    @classmethod
    def generate(cls, name, size, dist=ss.norm, **dist_kwargs):
        """Generates a column of random data"""
        dist_kwargs = dist_kwargs or {'loc': 0., 'scale': 1.}
        arr = dist.rvs(**dist_kwargs, size=size)
        return cls(name=name, arr=arr)
