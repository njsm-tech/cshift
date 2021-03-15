from typing import List

import numpy as np
import pandas as pd

from cshift import constants
from cshift.core.compare.comparison import Comparison
from cshift.core.dataset import Dataset
from cshift.core.result.result import Result

class SummaryStatsComparison(Comparison):
    NUM_QUANTILES = 20  # 0-100
    PERCENTILES = np.arange(0, 100, NUM_QUANTILES)  # ints between 0 and 100
    PERCENTILES_NORMALIZED = PERCENTILES / 100.  # floats between 0. and 1.
    PERCENTILES_STR = [str(i) + '%' for i in PERCENTILES]

    DIFF_FIELDS = ['mean', 'std'] + PERCENTILES_STR

    @classmethod
    def compare(cls, 
            *datasets: List[Dataset],
            groupby_fields: List[str] = None) -> Result:
        cls.validate_datasets(*datasets, groupby_fields=groupby_fields)
        [ds1, ds2] = datasets
        ds1_summary = cls.compute_summary_stats(
            ds1, groupby_fields=groupby_fields)
        ds2_summary = cls.compute_summary_stats(
            ds2, groupby_fields=groupby_fields)
        diff = ds1_summary - ds2_summary
        return Result(diff)

    @classmethod
    def compute_summary_stats(cls,
            dataset: Dataset,
            groupby_fields: List[str] = None) -> pd.DataFrame:
        df = dataset.df
        if groupby_fields:
            desc = df.groupby(groupby_fields).describe(percentiles=cls.PERCENTILES_NORMALIZED)
            desc = desc.swaplevel(-1, -2, axis='columns').stack()
        else:
            desc = df.describe(percentiles=cls.PERCENTILES_NORMALIZED)
            desc = desc.swapaxes(0, 1)
        desc.index.names = desc.index.names[:-1] + [constants.COLNAME_FEATURE]
        return desc.loc[:, cls.DIFF_FIELDS]

    @classmethod
    def shift_detected(cls, 
            *datasets: List[Dataset],
            groupby_fields: List[str] = None) -> bool:
        diff = cls.compare(*datasets).df
        zeros = np.zeros_like(diff.values)
        return np.any(np.logical_not(np.isclose(diff.values, zeros, atol=cls.ATOL)))
