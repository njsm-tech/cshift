from typing import Dict

from flask import Flask, request

from google.cloud import pubsub_v1
from google.protobuf.json_format import MessageToJson

from cshift.client_service_common import api_paths
from cshift.client_service_common import config as csc_config
from cshift.core.compare.comparison_pipeline import ComparisonPipeline
from cshift.proto import cshift_pb2 as pb2

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(
    csc_config.PROJECT, csc_config.COMPARISONS_SUBSCRIPTION_ID)

def callback(message):
    try:
        print(f"Received {message}.")
        job_id = message.attributes['job_id']
        spec = pb2.ComparisonPipelineSpec()
        spec.ParseFromString(message.data)
        pipeline = ComparisonPipeline.from_spec(spec)
        result_set = pipeline.run(job_id=job_id)
        result_set.record()
        print(f"Recorded result_set {result_set} for job {job_id}")
    except Exception as e:
        print(f"Threw exception {e} on job {job_id}")
    finally:
        message.ack()

def main():
    streaming_pull_future = subscriber.subscribe(
        subscription_path,
        callback=callback)
    print(f"Listening for messages on {subscription_path}..\n")

    # Wrap subscriber in a 'with' block to automatically call close() when done.
    with subscriber:
        try:
            # When `timeout` is not set, result() will block indefinitely,
            # unless an exception is encountered first.
            streaming_pull_future.result()
        except TimeoutError:
            streaming_pull_future.cancel()

if __name__ == '__main__':
    main()
    # app.run(host=api_paths.HOST, port=api_paths.COMPUTE_PORT, debug=True)