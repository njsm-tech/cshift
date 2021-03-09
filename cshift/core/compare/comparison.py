from typing import List

import pandas as pd

from cshift.core import compare
from cshift.core.dataset import Dataset
from cshift.proto import cshift_pb2 as pb2

class Comparison:
    ATOL = 1e-1  # absolute tolerance for np.isclose

    @staticmethod
    def from_enum(comparison_type: pb2.ComparisonType):
        if comparison_type == pb2.ComparisonType.SUMMARY_STATS:
            return compare.summary_stats.SummaryStatsComparison
        elif comparison_type == pb2.ComparisonType.KS:
            return compare.ks.KSComparison
        elif comparison_type == pb2.ComparisonType.LR:
            return compare.lr.LRComparison

    @classmethod
    def compare(cls, 
            *datasets: List[Dataset], 
            groupby_fields: List[str] = None) -> pd.DataFrame:
        """
        Checks for distributional shift between datasets using the 
            comparison specified by the subclass. 
        Returns pandas dataframe showing the computed distributional 
            difference between the datasets, using the metrics 
            specified by each subclass. 

        Implemented by subclasses.
        """
        raise NotImplementedError()

    @classmethod
    def shift_detected(cls, 
            *datasets: List[Dataset],
            groupby_fields: List[str] = None) -> bool:
        """
        Checks for distributional shift between datasets using the 
            comparison specified by the subclass. 
        Returns bool: True if shift is detected, False if shift is
            not detected. 
        This method can be considered to be a reduction of 'compare'.

        Implemented by subclasses.
        """
        raise NotImplementedError()

    @classmethod
    def validate_datasets(cls, 
            *datasets: List[Dataset],
            groupby_fields: List[str] = None) -> None:
        if len(datasets) != 2:
            raise ValueError("Require exactly 2 datasets; got %d" % len(datasets))

    @property
    def name(self):
        return self.__class__.__name__
