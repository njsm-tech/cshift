from typing import List, Tuple

import numpy as np
import pandas as pd

from ..column import Column

class Dataset:
    def __init__(self, df: pd.DataFrame):
        self.df: pd.DataFrame = df

    @classmethod
    def concatenate(cls, *dss):
        dfs = map(lambda ds: ds.df, dss)
        df = pd.concat(dfs, axis=0)
        return cls(df)

    @classmethod
    def from_array(cls, arr: np.NDArray, colnames: List[str]):
        df: pd.DataFrame = pd.DataFrame(data=arr, columns=colnames)
        return cls(df=df)

    @classmethod
    def from_columns(cls, columns: List[Column]):
        names, arrays = [], []
        for col in columns:
            names.append(col.name)
            arrays.append(col.arr)
        data = np.vstack(arrays)
        df = pd.DataFrame(data=data, columns=names)
        return cls(df)

    @classmethod
    def read_from_file(cls, path, **kwargs):
        df = pd.read_parquet(path, **kwargs)
        return cls(df)

    def write_to_file(self, path, **kwargs):
        self.df.to_parquet(path=path, **kwargs)
