from flask import Flask, request

from cshift.client_service_common import api_paths
from cshift.core.comparison_pipeline import ComparisonPipeline
from cshift.core.result.result_set import ResultSet
from cshift.proto import messages_pb2

app = Flask(__name__)

@app.route(api_paths.COMPUTE_COMPARISON, methods=['POST'])
def compute_comparison() -> ResultSet:
    spec = messages_pb2.ComparisonPipelineSpec()
    spec.ParseFromString(request.data)
    pipeline = ComparisonPipeline.from_spec(spec)
    result_set = pipeline.run()
    result_set.record()
    return result_set

if __name__ == '__main__':
    app.run(host=api_paths.HOST, port=api_paths.COMPUTE_PORT, debug=True)