import pytest

from cshift.client.client_comparison_pipeline import ClientComparisonPipeline
from cshift.client_service_common.result_set_future import ResultSetFuture

from conftest import client_comparison_pipeline

def test_submit(
        client_comparison_pipeline: ClientComparisonPipeline):
    res_future = client_comparison_pipeline.submit()
    assert isinstance(res_future, ResultSetFuture)
    print(res_future)