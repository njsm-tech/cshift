from typing import List, Tuple

import numpy as np
import pandas as pd

from .column import Column

class Dataset:
    def __init__(self, df: pd.DataFrame):
        self.df: pd.DataFrame = df

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
    def generate(cls, size: Tuple[int]):
        ncols = size[0]
        nrows = size[1]
        cols = []
        for i, col in enumerate(cols):
            name = 'col' + str(i)
            cols.append(Column.generate(name=name, size=nrows))
        return cls.from_columns(cols)
