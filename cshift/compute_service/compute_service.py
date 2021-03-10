from flask import Flask

from google.cloud import pubsub_v1

from cshift.client_service_common import api_paths
from cshift.client_service_common import config as csc_config
from cshift.core.comparison_pipeline import ComparisonPipeline
from cshift.core.result import Result
from cshift.proto import cshift_pb2 as pb2

app = Flask(__name__)

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(
    csc_config.PROJECT, api_paths.COMPARISONS_PUBSUB_TOPIC)
subscription_path = subscriber.subscription_path(
    csc_config.PROJECT, api_paths.COMPARISONS_SUBSCRIPTION_ID)

def callback(msg):
    print(msg.data)
    msg.ack()

@app.route(api_paths.COMPUTE_COMPARISON, methods=['POST'])
def compute_comparison() -> Result:
    future = subscriber.subscribe(
        subscription_path, callback)
    try:
        msg_bytes = future.result()
        spec = pb2.ComparisonPipelineSpec().ParseFromString(msg_bytes)
        pipeline = ComparisonPipeline.from_spec(spec)
        result = pipeline.run()
        # store result or return ?
        return result
    except KeyboardInterrupt:
        future.cancel()
    except Exception as e:
        print(e)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8081, debug=True)