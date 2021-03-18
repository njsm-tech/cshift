from flask import Flask, request

from cshift.client_service_common import api_paths
from cshift.proto import cshift_pb2 as pb2

from cshift.services.main_service import lib

app = Flask(__name__)

@app.route(api_paths.ROOT, methods=['GET'])
def root():
    return 'hello world'

@app.route(api_paths.REGISTER_DATASET, methods=['POST'])
def register_dataset():
    spec = pb2.DatasetSpec()
    spec.ParseFromString(request.data)
    return lib.register_dataset(spec)

@app.route(api_paths.REGISTER_MODEL, methods=['POST'])
def register_model():
    spec = pb2.ModelSpec()
    spec.ParseFromString(request.data)
    return lib.register_model(spec)

@app.route(api_paths.SUBMIT_COMPARISON, methods=['POST'])
def submit_comparison():
    spec = pb2.ComparisonPipelineSpec()
    spec.ParseFromString(request.data)
    return lib.submit_comparison(spec)

@app.route(api_paths.GET_RESULT, methods=['GET'])
def get_result():
    spec = pb2.ResultSpec()
    spec.ParseFromString(request.data)
    return lib.get_result(spec)

@app.route(api_paths.POLL_RESULT, methods=['GET'])
def poll_result():
    spec = pb2.ResultSpec()
    spec.ParseFromString(request.data)
    return lib.poll_result(spec)

@app.route(api_paths.RECORD_RESULT, methods=['POST'])
def record_result():
    spec = pb2.ResultSpec()
    spec.ParseFromString(request.data)
    return lib.record_result(spec)

if __name__ == '__main__':
    app.run(host=api_paths.HOST, port=api_paths.MAIN_PORT, debug=True)