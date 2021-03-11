from flask import Flask, request

from cshift.client_service_common import api_paths
from cshift.core.comparison_pipeline import ComparisonPipeline
from cshift.core.result import Result
from cshift.proto import cshift_pb2 as pb2

app = Flask(__name__)

@app.route(api_paths.COMPUTE_COMPARISON, methods=['POST'])
def compute_comparison() -> Result:
    spec = pb2.ComparisonPipelineSpec()
    spec.ParseFromString(request.data)
    pipeline = ComparisonPipeline.from_spec(spec)
    result = pipeline.run()
    return result

if __name__ == '__main__':
    app.run(host=api_paths.HOST, port=api_paths.COMPUTE_PORT, debug=True)