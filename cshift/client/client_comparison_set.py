from .client_comparison import ClientComparison
from .client_object import ClientObject

class ClientComparisonSet(ClientObject):
    def __init__(self,
                 *comparisons: ClientComparison,
                 **kwargs):
        super().__init__(**kwargs)
        self.comparisons = comparisons