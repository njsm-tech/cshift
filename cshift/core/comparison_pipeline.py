from typing import List

from cshift.proto import cshift_pb2 as pb2

from .compare import comparison_from_enum
from .compare.comparison import Comparison
from .dataset import Dataset
from cshift.core.result.result import Result
from cshift.core.result.result_set import ResultSet

class ComparisonPipeline:
    def __init__(self, 
            datasets: List[Dataset],
            index_fields: List[str],
            groupby_fields: List[str],
            comparisons: List[Comparison]):
        self.datasets = datasets
        self.index_fields = index_fields
        self.groupby_fields = groupby_fields
        self.comparisons = comparisons

    @classmethod
    def from_spec(cls, spec: pb2.ComparisonPipelineSpec):
        datasets = []
        for dataset_spec in spec.dataset_specs:
            ds = Dataset.from_spec(dataset_spec)
            datasets.append(ds)
        comparisons = []
        for comparison_type in spec.comparison_types:
            comp = comparison_from_enum(comparison_type)
            comparisons.append(comp)
        return cls(
            datasets=datasets,
            index_fields=list(spec.index_fields),
            groupby_fields=list(spec.groupby_fields),
            comparisons=comparisons)

    def run(self) -> ResultSet:
        result_set = ResultSet()
        for comp in self.comparisons:
            result: Result = comp.compare()
            result_set.add_result(result)
        return result_set
