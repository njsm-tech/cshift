from flask import Flask, request

from cshift.client_service_common import api_paths
from cshift.proto import cshift_pb2 as pb2

import lib

app = Flask(__name__)

@app.route(api_paths.ROOT)
def root():
    return 'hello world'

@app.route(api_paths.REGISTER_DATASET)
def register_dataset():
    spec = pb2.DatasetSpec().ParseFromString(request.data)
    return lib.register_dataset(spec)

@app.route(api_paths.REGISTER_MODEL)
def register_model():
    spec = pb2.ModelSpec().ParseFromString(request.data)
    return lib.register_model(spec)

@app.route(api_paths.SUBMIT_COMPARISON)
def submit_comparison():
    spec = pb2.ComparisonPipelineSpec().ParseFromString(request.data)
    return lib.compare_datasets(spec)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)