import pytest

from cshift.client.client_comparison_pipeline import ClientComparisonPipeline

from conftest import client_comparison_pipeline

def test_submit(
        client_comparison_pipeline: ClientComparisonPipeline):
    res = client_comparison_pipeline.submit()
    print(res.json())
    assert res.status_code == 200