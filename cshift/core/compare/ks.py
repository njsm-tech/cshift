from typing import List

import numpy as np
import pandas as pd
import scipy.stats as ss

from cshift.core.compare.comparison import Comparison
from cshift.core.dataset import Dataset
from cshift.core.result.result import Result
from cshift.proto import cshift_pb2 as pb2

class KSComparison(Comparison):
    comparison_type = pb2.ComparisonType.KS

    KS_STAT = 'ks_stat'
    KS_PVAL = 'ks_pval'
    KS_PVAL_THRESH = .05 

    def compare(self) -> Result:
        self.validate_datasets(
            *self.datasets,
            groupby_fields=self.groupby_fields)
        [ds1, ds2] = self.datasets
        df1, df2 = ds1.df, ds2.df
        res_map = {}
        for colname in df1.columns:
            arr1, arr2 = df1[colname].values, df2[colname].values
            (stat, pval) = ss.ks_2samp(arr1, arr2)
            res_map[colname] = {
                self.KS_STAT: stat,
                self.KS_PVAL: pval
            }
        res_df = pd.DataFrame.from_dict(res_map, orient='columns')
        return Result(df=res_df, comparison_spec=self.spec)

    def shift_detected(self) -> bool:
        """Looks for shift only using pvalues, as this should contain 
            all necessary information for shift detection."""
        diff = self.compare().df
        pvals = diff.loc[self.KS_PVAL].values
        return np.any(pvals < self.KS_PVAL_THRESH)
