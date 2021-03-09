from flask import Flask

from google.cloud import pubsub_v1

from cshift.client_service_common import api_paths

app = Flask(__name__)

publisher = pubsub_v1.PublisherClient()
subscriber = pubsub_v1.SubscriberClient()
topic_path = publisher.topic_path(
    api_paths.PROJECT, api_paths.COMPARISONS_PUBSUB_TOPIC)
subscription_path = subscriber.subscription_path(
    api_paths.PROJECT, api_paths.COMPARISONS_SUBSCRIPTION_ID)

def callback(msg):
    print(msg.data)
    msg.ack()

@app.route(api_paths.COMPUTE_COMPARISON)
def compute_comparison(msg):
    future = subscriber.subscribe(
        subscription_path, callback)
    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8081, debug=True)