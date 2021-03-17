from typing import List

from cshift import enums
from cshift.proto import cshift_pb2 as pb2

from .client_dataset import ClientDataset
from .client_object import ClientObject

class ClientComparison(ClientObject):
    def __init__(self,
                 *datasets: ClientDataset,
                 comparison_type: enums.ComparisonType,
                 groupby_fields: List[str] = None,
                 index_fields: List[str] = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.datasets = datasets
        self.comparison_type = comparison_type
        self.groupby_fields = groupby_fields
        self.index_fields = index_fields
        self.spec = pb2.ComparisonSpec(
            dataset_specs=[ds.spec for ds in datasets],
            groupby_fields=groupby_fields,
            index_fields=index_fields,
            comparison_type=comparison_type)