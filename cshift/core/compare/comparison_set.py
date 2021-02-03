from __future__ import annotations

from typing import List

import pandas as pd

from cshift.client_service_common.result_set_future import ResultSetFuture
from cshift.core.dataset import Dataset
from cshift.core.compare.comparison import Comparison
from cshift.core.compare.ks import KSComparison
from cshift.core.compare.lr import LRComparison
from cshift.core.compare.summary_stats import SummaryStatsComparison
from cshift.core.result.result_set import ResultSet
from cshift.proto import cshift_pb2 as pb2

from .comparison_interface import ComparisonInterface

class ComparisonSet(ComparisonInterface):
    """
    Convenience class to hold multiple comparisons.
    """
    DEFAULT_COMPARISONS = [
        SummaryStatsComparison,
        KSComparison,
        LRComparison
            ]

    def __init__(self, *comparisons: Comparison):
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
    def default(cls, *args, **kwargs) -> ComparisonSet:
        comparisons = [comp(*args, **kwargs) for comp in cls.DEFAULT_COMPARISONS]
        return cls(*comparisons)

    def compare(self, 
            *datasets: List[Dataset], 
            groupby_fields: List[str] = None) -> ResultSet:
        results = ResultSet()
        for comp in self.comparisons:
            res = comp.compare()
            results.add_result(res)
        return results

    def get_result_set_future(self) -> ResultSetFuture:
        pass

    def shift_detected(self, 
            *datasets: List[Dataset],
            groupby_fields: List[str] = None) -> bool:
        for comp in self.comparisons:
            if comp.shift_detected():
                return True
        return False

    @property
    def name(self) -> str:
        d = {
                'name': self.__class__.__name__,
                'members': self.comparisons
            }
        return "{name}({members})".format(**d)
