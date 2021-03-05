from typing import List

from .compare.comparison import Comparison
from .dataset import Dataset
from .result import Result

class Pipeline:
    def __init__(self, 
            datasets: List[Dataset],
            index_fields: List[str],
            groupby_fields: List[str],
            comparisons: List[Comparison]):
        self.datasets = datasets
        self.index_fields = index_fields
        self.groupby_fields = groupby_fields
        self.comparisons = comparisons

    def run(self) -> Result:
        result = Result()
        for comp in self.comparisons:
            result[comp.name] = comp.compare(
                    *self.datasets,
                    groupby_fields=self.groupby_fields)
        return result
