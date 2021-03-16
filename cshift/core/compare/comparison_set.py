from __future__ import annotations

from typing import List

import pandas as pd

from cshift.core.dataset import Dataset
from cshift.core.compare.comparison import Comparison
from cshift.core.compare.ks import KSComparison
from cshift.core.compare.lr import LRComparison
from cshift.core.compare.summary_stats import SummaryStatsComparison
from cshift.proto import cshift_pb2 as pb2

class ComparisonSet(Comparison):
    """
    Convenience class to hold multiple comparisons.
    """
    DEFAULT_COMPARISONS = [
        SummaryStatsComparison,
        KSComparison,
        LRComparison
            ]

    def __init__(self, *comparisons: Comparison, **kwargs):
        super().__init__(**kwargs)
        self.comparisons = comparisons
        self.spec = self.make_spec(*comparisons)

    @classmethod
    def make_spec(cls,
                  *comparisons: Comparison) -> pb2.ComparisonSetSpec:
        comparison_specs = []
        for comp in comparisons:
            comparison_specs.append(comp.spec)
        return pb2.ComparisonSetSpec(comparison_specs=comparison_specs)

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
