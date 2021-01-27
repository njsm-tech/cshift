from typing import List

import numpy as np
import pandas as pd

from .comparison import Comparison
from ..dataset import Dataset
from ..enums import SummaryStats

class SummaryStatsComparison(Comparison):
    NUM_QUANTILES = 20  # 0-100
    PERCENTILES = np.arange(0, 100, NUM_QUANTILES)  # ints between 0 and 100
    PERCENTILES_NORMALIZED = PERCENTILES / 100.  # floats between 0. and 1.
    PERCENTILES_STR = [str(i) + '%' for i in PERCENTILES]

    DIFF_FIELDS = ['mean', 'std'] + PERCENTILES_STR

    @classmethod
    def compare(cls, *datasets: List[Dataset]) -> pd.DataFrame:
        cls.validate_datasets(*datasets)
        [ds1, ds2] = datasets
        ds1_summary = cls.compute_summary_stats(ds1)
        ds2_summary = cls.compute_summary_stats(ds2)
        return ds1_summary - ds2_summary

    @classmethod
    def compute_summary_stats(cls, dataset: Dataset) -> pd.DataFrame:
        df = dataset.df
        desc = df.describe(percentiles=cls.PERCENTILES_NORMALIZED)
        return desc.loc[cls.DIFF_FIELDS]
