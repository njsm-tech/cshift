from __future__ import annotations

from typing import Dict, List

import numpy as np
import pandas as pd
from sklearn.model_selection import train_test_split

from cshift.core.column import Column
from cshift.dao.artifact import Artifact
from cshift.proto import cshift_pb2 as pb2

class Dataset:
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.spec = pb2.DatasetSpec() # TODO: fill this in!

    @classmethod
    def concatenate(cls, *dss: List[Dataset]):
        dfs = map(lambda ds: ds.df, dss)
        df = pd.concat(dfs, axis=0)
        return cls(df)

    @classmethod
    def from_array(cls, arr: np.array, colnames: List[str]):
        df = pd.DataFrame(data=arr, columns=colnames)
        return cls(df=df)

    @classmethod
    def from_columns(cls, columns: List[Column]):
        names, arrays = [], []
        for col in columns:
            names.append(col.name)
            arrays.append(col.arr)
        print(names)
        data = np.hstack(arrays)
        df = pd.DataFrame(data=data, columns=names)
        return cls(df)

    @classmethod
    def from_spec(cls, spec: pb2.DatasetSpec):
        artifact = Artifact(spec=spec.artifact_spec)
        parquet_bytes = artifact.download()
        df = artifact.dataframe_from_parquet_bytes(
            parquet_bytes=parquet_bytes)
        return cls(df=df)

    @classmethod
    def read(cls, path, **kwargs):
        df = pd.read_parquet(path, **kwargs)
        return cls(df)

    def write(self, path, **kwargs):
        self.df.to_parquet(path=path, **kwargs)

    def split(self, 
            split_conds: List[str] = None,
            rand_split_size: float = None,
            field_ord_split: Dict[str, float] = None) -> List[Dataset]:
        """
        Takes one of the following keyword arguments:
            1) split_conds: a list of 2 split conditions, which are 
                string literals of pandas indexing conditions. 
                Ex.:
                    ['col1 > 0.', 'col1 <= 0.']
            2) rand_split_size: a float r, 0. < r < 1., which will 
                randomly split the dataset into datasets of size r and (1.-r)
            3) field_ord_split: a dict mapping a field name to a float 
                r, 0. < r < 1., which will split the dataset into the 
                first r rows and last (1-r) rows of the dataset sorted by 
                the given field
        Returns two separate datasets according to the split. 
        """
        sc_nn = int(split_conds is not None)
        rss_nn = int(rand_split_size is not None)
        fos_nn = int(field_ord_split is not None)
        if sum([sc_nn, rss_nn, fos_nn]) != 1:
            raise ValueError("Please supply exactly one of "
                "'split_conds', 'rand_split_size', or 'field_ord_split'")
        if sc_nn:
            [cond1, cond2] = split_conds
            df1, df2 = self.df.query(cond1), self.df.query(cond2)
            return [Dataset(df1), Dataset(df2)]
        if rss_nn:
            df1, df2 = train_test_split(self.df, test_size=rand_split_size)
            return [Dataset(df1), Dataset(df2)]
        if fos_nn:
            if len(field_ord_split) != 1:
                raise ValueError("Please exactly only one split field")
            field, size = zip(*field_ord_split.items())
            field, size = field[0], size[0]
            sorted_df = self.df.sort_values(by=field)
            cutoff = int(len(sorted_df) * size)
            df1, df2 = sorted_df.iloc[:cutoff], sorted_df.iloc[cutoff:]
            return [Dataset(df1), Dataset(df2)]
