from .client_comparison_pipeline import ClientComparisonPipeline
from .client_object import ClientObject
from .client_result import ClientResult

class ClientResultSet(ClientObject):
    def __init__(self,
                 comparison_pipeline: ClientComparisonPipeline,
                 **kwargs):
        super().__init__(**kwargs)
        self.comparison_pipeline = comparison_pipeline
        results = []
        comparison_set = self.comparison_pipeline.to_client_comparison_set()
        for comp in comparison_set.comparisons:
            results.append(ClientResult(comp, **kwargs))
        self.results = results

    def poll(self):
        responses = []
        for res in self.results:
            responses.append(res.poll())
        return responses

    def get(self, wait=True):
        responses = []
        for res in self.results:
            responses.append(res.get(wait=wait))
        return responses
