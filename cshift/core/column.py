import numpy as np
import pandas as pd
import scipy.stats as ss

import pb2

class Column:
    # TODO: add type? 
    def __init__(self, name, arr):
        self.name = name
        self.arr = arr

    @classmethod
    def from_series(cls, series: pd.Series):
        pass

    @classmethod
    def generate_from_spec(cls, spec: pb2.ColumnSpec):
        rspec = spec.random_column_spec
        dist = getattr(ss, rspec.dist_name)
        data = dist.rvs(size=rspec.size, **rspec.kwargs)
        return cls(name=spec.name, arr=data)
