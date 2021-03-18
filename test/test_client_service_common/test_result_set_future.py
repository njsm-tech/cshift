import pytest

from conftest import client_comparison_pipeline

from cshift.client.client_comparison_pipeline import ClientComparisonPipeline
from cshift.client_service_common.result_set_future import ResultSetFuture

SCOPE = 'package'

@pytest.fixture(scope=SCOPE)
def result_set_future(client_comparison_pipeline: ClientComparisonPipeline) -> ResultSetFuture:
    return client_comparison_pipeline.submit()

def test_get(result_set_future: ResultSetFuture):
    print(result_set_future.get())
    assert result_set_future is not None