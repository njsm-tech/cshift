from typing import List

from cshift.proto import cshift_pb2 as pb2

from cshift.core.compare import comparison_from_enum
from cshift.core.compare.comparison_set import ComparisonSet
from cshift.core.dataset import Dataset
from cshift.dao.job import Job
from cshift.core.result.result import Result
from cshift.core.result.result_set import ResultSet
from cshift import enums

class ComparisonPipeline:
    def __init__(self,
                 *datasets: Dataset,
                 comparison_types: List[enums.ComparisonType],
                 groupby_fields: List[str] = None,
                 index_fields: List[str] = None):
        self.datasets = datasets
        self.comparison_types = comparison_types
        self.groupby_fields = groupby_fields
        self.index_fields = index_fields
        comparisons = []
        for ct in comparison_types:
            comp_cls = comparison_from_enum(ct)
            comparisons.append(comp_cls(
                *self.datasets,
                groupby_fields=self.groupby_fields,
                index_fields=self.index_fields
            ))
        self.comparisons = comparisons

    @classmethod
    def from_spec(cls, spec: pb2.ComparisonPipelineSpec):
        datasets = []
        for dataset_spec in spec.dataset_specs:
            ds = Dataset.from_spec(dataset_spec)
            datasets.append(ds)
        return cls(
            *datasets,
            comparison_types=list(spec.comparison_types),
            index_fields=list(spec.index_fields),
            groupby_fields=list(spec.groupby_fields))

    def run(self, job_id: str = None) -> ResultSet:
        job = Job(job_id=job_id)
        job.mark_status(enums.JobStatus.STARTED)
        result_set = ResultSet()
        for comp in self.comparisons:
            result: Result = comp.compare()
            result_set.add_result(result)
        job.mark_status(enums.JobStatus.COMPLETED)
        return result_set

    def to_comparison_set(self) -> ComparisonSet:
        return ComparisonSet(*self.comparisons)
