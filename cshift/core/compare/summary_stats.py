from typing import List

import numpy as np
import pandas as pd

from .comparison import Comparison
from ..dataset import Dataset
from ..enums import SummaryStats

RTOL = 1e-2

class SummaryStatsComparison(Comparison):
    NUM_QUANTILES = 20  # 0-100
    PERCENTILES = np.arange(0, 100, NUM_QUANTILES)  # ints between 0 and 100
    PERCENTILES_NORMALIZED = PERCENTILES / 100.  # floats between 0. and 1.
    PERCENTILES_STR = [str(i) + '%' for i in PERCENTILES]

    DIFF_FIELDS = ['mean', 'std'] + PERCENTILES_STR

    @classmethod
    def datasets_same(cls, *datasets: List[Dataset]) -> bool:
        if len(datasets) != 2:
            raise ValueError("Require exactly 2 datasets; got %d" % len(datasets))
        
        [ds1, ds2] = datasets
        ds1_summary = cls.compute_summary_stats(ds1)
        ds2_summary = cls.compute_summary_stats(ds2)
        # isclose gives a detailed comparison 
        # should we return that instead? 
        isclose = np.isclose(ds1_summary, ds2_summary, rtol=RTOL)
        return np.all(isclose)

    @classmethod
    def compute_summary_stats(cls, dataset: Dataset) -> pd.DataFrame:
        df = dataset.df
        desc = df.describe(percentiles=cls.PERCENTILES_NORMALIZED)
        return desc.loc[cls.DIFF_FIELDS]
