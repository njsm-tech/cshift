from typing import List

from .dataset import Dataset
from .comparison import Comparison

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

    def run(self) -> None:


