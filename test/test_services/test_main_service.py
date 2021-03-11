from typing import Dict, List

from flask import Flask

from cshift.client.client_comparison_pipeline import ClientComparisonPipeline
from cshift.client.client_dataset import ClientDataset
from cshift.client.client_model import ClientModel

from conftest import main_service_app, comparison_pipelines, datasets, model

def test_register_dataset(main_service_app: Flask, datasets: Dict[str, ClientDataset]):
    for (name, dataset) in datasets.items():
        resp = dataset.register()
        print(resp)

def test_register_model(main_service_app: Flask, model: ClientModel):
    resp = model.register()
    print(resp)

def test_submit_comparison(
        main_service_app: Flask,
        comparison_pipelines: List[ClientComparisonPipeline]):
    for cp in comparison_pipelines:
        resp = cp.submit()
        print(resp)