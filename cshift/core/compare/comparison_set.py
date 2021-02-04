from __future__ import annotations

from typing import List

import pandas as pd

from .comparison import Comparison
from .ks import KSComparison
from .lr import LRComparison
from .summary_stats import SummaryStatsComparison

class ComparisonSet(Comparison):
    """
    Convenience class to hold multiple comparisons.
    """
    DEFAULT_COMPARISONS = [
        SummaryStatsComparison,
        KSComparison,
        LRComparison
            ]

    def __init__(self, *comparisons: List[Comparison]):
        self.comparisons = comparisons 

    @classmethod
    def default(cls) -> ComparisonSet:
        return cls(*cls.DEFAULT_COMPARISONS)

    def compare(self, 
            *datasets: List[Dataset], 
            groupby_fields: List[str] = None) -> pd.DataFrame:
        results = []
        for comp in self.comparisons:
            res = comp.compare(*datasets, groupby_fields=groupby_fields)
            results.append(res)
        return pd.concat(results, axis=1)

    def shift_detected(self, 
            *datasets: List[Dataset],
            groupby_fields: List[str] = None) -> bool:
        for comp in self.comparisons:
            if comp.shift_detected(*datasets, groupby_fields=groupby_fields):
                return True
        return False

    @property
    def name(self) -> str:
        d = {
                'name': self.__class__.__name__,
                'members': self.comparisons
            }
        return "{name}({members})".format(**d)
