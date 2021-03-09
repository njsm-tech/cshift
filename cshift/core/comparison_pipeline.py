from typing import List

from cshift.proto import cshift_pb2 as pb2

from .compare.comparison import Comparison
from .dataset import Dataset
from .result import Result

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
            comp = Comparison.from_enum(comparison_type)
            comparisons.append(comp)
        return cls(
            datasets=datasets,
            index_fields=spec.index_fields,
            groupby_fields=spec.groupby_fields,
            comparisons=comparisons)

    def run(self) -> Result:
        result = Result()
        for comp in self.comparisons:
            result[comp.name] = comp.compare(
                    *self.datasets,
                    groupby_fields=self.groupby_fields)
        return result
