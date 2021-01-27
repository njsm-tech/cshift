from typing import List

import numpy as np
import pandas as pd
import scipy.stats as ss

from .comparison import Comparison
from ..dataset import Dataset

class KSComparison(Comparison):
    @classmethod
    def compare(cls, *datasets: List[Dataset]) -> pd.DataFrame:
        cls.validate_datasets(*datasets)
        [ds1, ds2] = datasets
        df1, df2 = ds1.df, ds2.df
        res_map = {}
        for colname in df1.columns:
            print(colname)
            arr1, arr2 = df1[colname].values, df2[colname].values
            (stat, pval) = ss.ks_2samp(arr1, arr2)
            res_map[colname] = {'ks_stat': stat, 'ks_pval': pval}
        res_df = pd.DataFrame.from_dict(res_map, orient='columns')
        print(res_df)
        return res_df

