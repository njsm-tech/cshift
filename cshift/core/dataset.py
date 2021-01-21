from typing import List

import numpy as np
import pandas as pd

class Dataset:
    def __init__(self, df: pd.DataFrame):
        self.df: pd.DataFrame = df

    @classmethod
    def from_array(cls, arr: np.NDArray, colnames: List[str]):
        df: pd.DataFrame = pd.DataFrame(data=arr, columns=colnames)
        return cls(df=df)
