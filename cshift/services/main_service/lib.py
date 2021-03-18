from typing import Dict

from uuid import uuid4

import requests

from google.cloud import datastore, pubsub_v1
from google.protobuf.json_format import MessageToDict

from cshift.proto import cshift_pb2 as pb2
from cshift.client_service_common import api_paths, utils
from cshift.client_service_common import config as csc_config

datastore_client = datastore.Client(project=csc_config.PROJECT)
publisher_client = pubsub_v1.PublisherClient()
comparisons_topic_path = publisher_client.topic_path(
    csc_config.PROJECT, csc_config.COMPARISONS_TOPIC)

def put_proto_to_datastore(key, msg):
    ent = datastore.Entity(key)
    d = MessageToDict(msg)
    for key, value in d.items():
        ent[key] = value
    datastore_client.put(ent)

def success_response() -> Dict:
    return {'status_code': 200}

def register_dataset(dataset_spec: pb2.DatasetSpec) -> Dict:
    key = datastore_client.key(
        csc_config.DATASETS_KEY,
        dataset_spec.metadata.id)
    put_proto_to_datastore(key, dataset_spec)
    return success_response()

def register_model(model_spec: pb2.ModelSpec) -> Dict:
    key = datastore_client.key(
        csc_config.MODELS_KEY,
        model_spec.metadata.id)
    put_proto_to_datastore(key, model_spec)
    return success_response()

def submit_comparison(comparison_spec: pb2.ComparisonPipelineSpec) -> Dict:
    # # TODO: instead of returning result_set directly, queue task and
    # #   have ClientResult track status of computation
    # res = requests.post(
    #     url=api_paths.compute_service_urlify(api_paths.COMPUTE_COMPARISON),
    #     headers={'Content-Type': 'application/protobuf'},
    #     data=comparison_spec.SerializeToString())
    # if res.status_code == 200:
    #     return success_response()
    # raise Exception('Request failed with status code %d' % res.status_code)
    spec_data = comparison_spec.SerializeToString()
    job_id = utils.make_id(comparison_spec)
    future = publisher_client.publish(
        comparisons_topic_path, spec_data, job_id=job_id)
    return {'status_code': 200, 'job_id': job_id}

def get_result(result_spec: pb2.ResultSpec) -> Dict:
    key = datastore_client.key(
        csc_config.RESULTS_KEY,
        result_spec.metadata.id)
    result = datastore_client.Entity(key)
    d = success_response()
    d['result'] = result
    return d

def poll_result(result_spec: pb2.ResultSpec) -> Dict:
    key = datastore_client.key(
        csc_config.RESULTS_KEY,
        result_spec.metadata.id)
    result = datastore_client.Entity(key)
    return result

def record_result(result_spec: pb2.ResultSpec) -> Dict:
    key = datastore_client.key(
        csc_config.RESULTS_KEY,
        result_spec.metadata.id)
    put_proto_to_datastore(key, result_spec)
    return success_response()