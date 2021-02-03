from __future__ import annotations

from typing import List

from cshift.client_service_common import api_paths, utils, result_set_future
from cshift import enums
from cshift.dao.job import Job
from cshift.proto import cshift_pb2 as pb2

from .client_comparison import ClientComparison
from .client_comparison_set import ClientComparisonSet
from .client_dataset import ClientDataset
from .client_config import ClientConfig
from .client_object import ClientObject

class ClientComparisonPipeline(ClientObject):
    def __init__(self,
                 datasets: List[ClientDataset],
                 index_fields: List[str],
                 groupby_fields: List[str],
                 comparison_types: List[str] = None,
                 config: ClientConfig = None):
        super().__init__(config=config)
        self._check_dataset_specs(datasets)

        self.datasets = datasets
        self.index_fields = index_fields
        self.groupby_fields = groupby_fields
        self.comparison_types = comparison_types

        self.dataset_specs = [ds.spec for ds in datasets]
        comparison_types = comparison_types or enums.ComparisonType.get_all_values()
        self.comparison_types = [utils.enum_str2pb(ct, pb2.ComparisonType) for ct in comparison_types]
        self.spec = pb2.ComparisonPipelineSpec(
            index_fields=self.index_fields,
            groupby_fields=self.groupby_fields,
            comparison_types=self.comparison_types,
            dataset_specs=self.dataset_specs)

    @property
    def result_set_spec(self) -> pb2.ResultSetSpec:
        client_comparison_set: ClientComparisonSet = self.to_client_comparison_set()
        return pb2.ResultSetSpec(
            comparison_set_spec=client_comparison_set.spec)

    def to_client_comparison_set(self) -> ClientComparisonSet:
        comparisons = []
        for ct in self.comparison_types:
            comp = ClientComparison(
                *self.datasets,
                comparison_type=ct,
                groupby_fields=self.groupby_fields,
                index_fields=self.index_fields)
            comparisons.append(comp)
        return ClientComparisonSet(*comparisons)

    def submit(self) -> result_set_future.ResultSetFuture:
        resp = self.request_post(
            url=api_paths.SUBMIT_COMPARISON,
            spec=self.spec).json()
        job = Job(job_id=resp['job_id'])
        future = result_set_future.ResultSetFuture(
            job, self.result_set_spec)
        return future

    def _check_dataset_specs(self,
                             dataset_specs: List[pb2.DatasetSpec]):
        if len(dataset_specs) != 2:
            raise ValueError(
                "Please supply exactly 2 datasets for comparison. " 
                "Got %d" % len(dataset_specs))