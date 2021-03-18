from typing import Dict, List

from cshift.client.client_comparison_pipeline import ClientComparisonPipeline
from cshift.client.client_dataset import ClientDataset
from cshift.client.client_model import ClientModel

from conftest import client_comparison_pipelines, client_datasets, client_model

def test_register_dataset(
        client_datasets: Dict[str, ClientDataset]):
    for (name, dataset) in client_datasets.items():
        resp = dataset.register()

def test_register_model(
        client_model: ClientModel):
    resp = client_model.register()

def test_submit_comparison(
        client_comparison_pipelines: List[ClientComparisonPipeline]):
    for cp in client_comparison_pipelines:
        resp = cp.submit()