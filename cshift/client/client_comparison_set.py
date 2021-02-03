from cshift.proto import cshift_pb2 as pb2

from .client_comparison import ClientComparison
from .client_object import ClientObject

class ClientComparisonSet(ClientObject):
    def __init__(self,
                 *comparisons: ClientComparison,
                 **kwargs):
        super().__init__(**kwargs)
        self.comparisons = comparisons
        comparison_specs = [c.spec for c in comparisons]
        self.spec = pb2.ComparisonSetSpec(
            comparison_specs=comparison_specs)