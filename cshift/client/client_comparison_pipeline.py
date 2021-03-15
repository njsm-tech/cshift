from __future__ import annotations

from typing import List

from cshift.client_service_common import api_paths, utils
from cshift.proto import enums_pb2, messages_pb2

from .client_dataset import ClientDataset
from .client_object import ClientObject

class ClientComparisonPipeline(ClientObject):
    def __init__(self,
                 datasets: List[ClientDataset],
                 index_fields: List[str],
                 groupby_fields: List[str],
                 comparison_types: List[str]):
        self._check_dataset_specs(datasets)
        dataset_specs = [ds.spec for ds in datasets]
        comparison_types = [utils.enum_str2pb(ct, enums_pb2.ComparisonType) for ct in comparison_types]
        self.spec = pb2.ComparisonPipelineSpec(
            index_fields=index_fields,
            groupby_fields=groupby_fields,
            comparison_types=comparison_types,
            dataset_specs=dataset_specs)

    def submit(self):
        return self.request_post(
            url=api_paths.SUBMIT_COMPARISON,
            spec=self.spec)

    def _check_dataset_specs(self,
                             dataset_specs: List[messages_pb2.DatasetSpec]):
        if len(dataset_specs) != 2:
            raise ValueError(
                "Please supply exactly 2 datasets for comparison. " 
                "Got %d" % len(dataset_specs))