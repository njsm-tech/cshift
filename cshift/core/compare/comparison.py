from __future__ import annotations

from typing import List

from cshift.core.dataset import Dataset
from cshift.core.result.result import Result
from cshift.proto import cshift_pb2 as pb2

from .comparison_interface import ComparisonInterface

class Comparison(ComparisonInterface):
    ATOL: float = 1e-1  # absolute tolerance for np.isclose
    comparison_type: pb2.ComparisonType = None # class property set by subclass

    def __init__(self,
                 *datasets: Dataset,
                 groupby_fields: List[str] = None,
                 index_fields: List[str] = None):
        self.datasets = datasets
        self.groupby_fields = groupby_fields
        self.index_fields = index_fields
        self.spec = self.__class__.make_spec(
            datasets=list(datasets),
            groupby_fields=groupby_fields,
            index_fields=index_fields)

    def compare(self) -> Result:
        raise NotImplementedError()

    def shift_detected(self) -> bool:
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
            comparison_type=cls.comparison_type)

    @classmethod
    def validate_datasets(cls, 
            *datasets: List[Dataset],
            groupby_fields: List[str] = None) -> None:
        if len(datasets) != 2:
            raise ValueError("Require exactly 2 datasets; got %d" % len(datasets))

    @property
    def name(self):
        return self.__class__.__name__