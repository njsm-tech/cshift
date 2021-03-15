import requests

from google.cloud import datastore
from google.protobuf.json_format import MessageToDict

from cshift import enums
from cshift.proto import cshift_pb2 as pb2
from cshift.client_service_common import api_paths

PROJECT = 'cshift'
COMPARISONS = 'comparisons'
DATASETS = 'datasets'
MODELS = 'models'

datastore_client = datastore.Client(project=PROJECT)

def put_proto_to_datastore(key, msg):
    ent = datastore.Entity(key)
    d = MessageToDict(msg)
    for key, value in d.items():
        ent[key] = value
    datastore_client.put(ent)

def register_dataset(dataset_spec: pb2.DatasetSpec):
    key = datastore_client.key(DATASETS, dataset_spec.name)
    put_proto_to_datastore(key, dataset_spec)
    return enums.ResponseCode.SUCCESS

def register_model(model_spec: pb2.ModelSpec):
    key = datastore_client.key(MODELS, model_spec.name)
    put_proto_to_datastore(key, model_spec)
    return enums.ResponseCode.SUCCESS

def submit_comparison(comparison_spec: pb2.ComparisonPipelineSpec):
    # TODO: instead of returning result_set directly, queue task and
    #   have ClientResult track status of computation
    return requests.post(
        url=api_paths.compute_service_urlify(api_paths.COMPUTE_COMPARISON),
        headers={'Content-Type': 'application/protobuf'},
        data=comparison_spec.SerializeToString())