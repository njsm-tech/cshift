from flask import Flask

from cshift.client_service_common import api_paths

import lib

app = Flask(__name__)

@app.route(api_paths.ROOT)
def root():
    return 'hello world'

@app.route(api_paths.REGISTER_DATASET)
def register_dataset(*args, **kwargs):
    return lib.register_dataset(*args, **kwargs)

@app.route(api_paths.REGISTER_MODEL)
def register_model(*args, **kwargs):
    return lib.register_model(*args, **kwargs)

@app.route(api_paths.SUBMIT_COMPARISON)
def submit_comparison(*args, **kwargs):
    return lib.compare_datasets(*args, **kwargs)

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=8080, debug=True)