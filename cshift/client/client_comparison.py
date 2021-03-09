from typing import List

from cshift.proto import cshift_pb2 as pb2

class ClientComparison:
    def __init__(self,
                 index_fields: List[str],
                 comparison_types: List[pb2.ComparisonType],
                 dataset_specs: List[pb2.DatasetSpec]):
        self._check_dataset_specs(dataset_specs)
        self.comparison_spec = pb2.ComparisonSpec(
            index_fields=index_fields,
            comparison_types=comparison_types,
            dataset_specs=dataset_specs
        )

    def _check_dataset_specs(self,
                             dataset_specs: List[pb2.DatasetSpec]):
        if len(dataset_specs) != 2:
            raise ValueError(
                "Please supply exactly 2 datasets for comparison. " 
                "Got %d" % len(dataset_specs))