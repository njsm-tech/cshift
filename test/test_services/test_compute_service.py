from typing import List

from cshift.client.client_comparison_pipeline import ClientComparisonPipeline
from cshift.client_service_common.result_set_future import ResultSetFuture

from conftest import client_comparison_pipelines

def test_compute_comparison(client_comparison_pipelines: List[ClientComparisonPipeline]):
    futures = []
    for cp in client_comparison_pipelines:
        res: ResultSetFuture = cp.submit()
        futures.append(res)
    for future in futures:
        print(future.get())