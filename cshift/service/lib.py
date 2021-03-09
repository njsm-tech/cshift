from google.cloud import datastore, pubsub_v1

from cshift.proto import cshift_pb2 as pb2
from cshift.client_service_common import api_paths

PROJECT = 'cshift'
COMPARISONS = 'comparisons'
DATASETS = 'datasets'
MODELS = 'models'

datastore_client = datastore.Client(project=PROJECT)
pubsub_client = pubsub_v1.PublisherClient()
comparisons_topic_path = pubsub_client.topic_path(
    PROJECT, api_paths.COMPARISONS_PUBSUB_TOPIC)

def put_proto(key, msg):
    ent = datastore.Entity(key)
    for descriptor in msg.DESCRIPTOR.fields:
        value = getattr(msg, descriptor.name, None)
        ent[descriptor.name] = value
    datastore_client.put(ent)

def register_dataset(dataset_spec: pb2.DatasetSpec):
    key = datastore_client.key(DATASETS)
    put_proto(key, dataset_spec)

def register_model(model_spec: pb2.ModelSpec):
    key = datastore_client.key(MODELS)
    put_proto(key, model_spec)

def compare_datasets(comparison_spec: pb2.ComparisonPipelineSpec):
    pubsub_client.publish(
        comparisons_topic_path,
        comparison_spec.SerializeToString())