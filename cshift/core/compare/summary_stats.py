from typing import List

from .comparison import Comparison
from ..dataset import Dataset
from ..enums import SummaryStats

class SummaryStatsComparison(Comparison):
    NUM_QUANTILES = 20 
    PERCENTILES = list(range(0, 100, NUM_QUANTILES)) / 100.

    def compare(self, *datasets: List[Dataset]):
        if len(datasets) != 2:
            raise ValueError("Require exactly 2 datasets; got %d" % len(datasets))

        ds1_summary_stats = self.

    def compute_summary_stats(self, dataset: Dataset):
        df = dataset.df
        desc = df.describe(percentiles=PERCENTILES)
