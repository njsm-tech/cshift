from typing import List

import numpy as np
import pandas as pd
import scipy.stats as ss

from cshift.core.compare.comparison import Comparison
from cshift.core.dataset import Dataset
from cshift.core.result.result import Result

class KSComparison(Comparison):
    KS_STAT = 'ks_stat'
    KS_PVAL = 'ks_pval'
    KS_PVAL_THRESH = .05 

    @classmethod
    def compare(cls, 
            *datasets: List[Dataset],
            groupby_fields: List[str] = None) -> Result:
        cls.validate_datasets(*datasets)
        [ds1, ds2] = datasets
        df1, df2 = ds1.df, ds2.df
        res_map = {}
        for colname in df1.columns:
            arr1, arr2 = df1[colname].values, df2[colname].values
            (stat, pval) = ss.ks_2samp(arr1, arr2)
            res_map[colname] = {cls.KS_STAT: stat, cls.KS_PVAL: pval}
        res_df = pd.DataFrame.from_dict(res_map, orient='columns')
        return Result(res_df)

    @classmethod
    def shift_detected(cls, 
            *datasets: List[Dataset],
            groupby_fields: List[str] = None) -> bool:
        """Looks for shift only using pvalues, as this should contain 
            all necessary information for shift detection."""
        diff = cls.compare(*datasets).df
        pvals = diff.loc[cls.KS_PVAL].values
        return np.any(pvals < cls.KS_PVAL_THRESH)
