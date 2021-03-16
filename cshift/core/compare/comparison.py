from __future__ import annotations

from typing import List

from cshift.core.dataset import Dataset
from cshift.core.result.result import Result
from cshift.proto import cshift_pb2 as pb2

class Comparison:
    ATOL = 1e-1  # absolute tolerance for np.isclose

    def __init__(self,
                 *datasets: Dataset,
                 groupby_fields: List[str] = None,
                 index_fields: List[str] = None):
        self.datasets = datasets
        self.groupby_fields = groupby_fields
        self.index_fields = index_fields
        self.spec = self.make_spec(
            datasets=list(datasets),
            groupby_fields=groupby_fields,
            index_fields=index_fields)

    def compare(self) -> Result:
        """
        Checks for distributional shift between datasets using the 
            comparison specified by the subclass. 
        Returns pandas dataframe showing the computed distributional 
            difference between the datasets, using the metrics 
            specified by each subclass. 

        Implemented by subclasses.
        """
        raise NotImplementedError()

    def shift_detected(self) -> bool:
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
    def make_spec(cls,
                  datasets: List[Dataset] = None,
                  groupby_fields: List[str] = None,
                  index_fields: List[str] = None) -> pb2.ComparisonSpec:
        return pb2.ComparisonSpec(
            dataset_specs=[ds.spec for ds in datasets],
            groupby_fields=groupby_fields,
            index_fields=index_fields,
            comparison_type=cls.COMPARISON_TYPE)

    @classmethod
    def validate_datasets(cls, 
            *datasets: List[Dataset],
            groupby_fields: List[str] = None) -> None:
        if len(datasets) != 2:
            raise ValueError("Require exactly 2 datasets; got %d" % len(datasets))

    @property
    def name(self):
        return self.__class__.__name__
