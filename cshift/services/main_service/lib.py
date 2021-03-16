from typing import Dict

import requests

from google.cloud import datastore
from google.protobuf.json_format import MessageToDict

from cshift.proto import cshift_pb2 as pb2
from cshift.client_service_common import api_paths
from cshift.client_service_common import config as csc_config

datastore_client = datastore.Client(project=csc_config.PROJECT)

def put_proto_to_datastore(key, msg):
    ent = datastore.Entity(key)
    d = MessageToDict(msg)
    for key, value in d.items():
        ent[key] = value
    datastore_client.put(ent)

def success_response() -> Dict:
    return {'status_code': 200}

def register_dataset(dataset_spec: pb2.DatasetSpec):
    key = datastore_client.key(csc_config.DATASETS_KEY, dataset_spec.name)
    put_proto_to_datastore(key, dataset_spec)
    return success_response()

def register_model(model_spec: pb2.ModelSpec):
    key = datastore_client.key(csc_config.MODELS_KEY, model_spec.name)
    put_proto_to_datastore(key, model_spec)
    return success_response()

def submit_comparison(comparison_spec: pb2.ComparisonPipelineSpec):
    # TODO: instead of returning result_set directly, queue task and
    #   have ClientResult track status of computation
    res = requests.post(
        url=api_paths.compute_service_urlify(api_paths.COMPUTE_COMPARISON),
        headers={'Content-Type': 'application/protobuf'},
        data=comparison_spec.SerializeToString())
    if res.status_code == 200:
        return success_response()
    raise Exception('Request failed with status code %d' % res.status_code)

def get_result(result_spec: pb2.ResultSpec):
    key = datastore_client.key(csc_config.RESULTS_KEY, result_spec.name)

def record_result(result_spec: pb2.ResultSpec):
    key = datastore_client.key(csc_config.RESULTS_KEY, result_spec.name)
    put_proto_to_datastore(key, result_spec)
    return success_response()