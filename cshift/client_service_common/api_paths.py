HOST = '127.0.0.1'
MAIN_PORT = 8080
COMPUTE_PORT = 8081
MAIN_ENDPOINT = f'http://{HOST}:{MAIN_PORT}'
COMPUTE_ENDPOINT = f'http://{HOST}:{COMPUTE_PORT}'

ROOT = '/'

COMPARISONS = ROOT + 'comparisons'
COMPUTE = ROOT + 'compute'
DATASETS = ROOT + 'datasets'
MODELS = ROOT + 'models'
RESULTS = ROOT + 'results'

GET_RESULT = '{}/get'.format(RESULTS)
POLL_RESULT = '{}/poll'.format(RESULTS)
RECORD_RESULT = '{}/record'.format(RESULTS)
REGISTER_DATASET = '{}/register'.format(DATASETS)
REGISTER_MODEL = '{}/register'.format(MODELS)
SUBMIT_COMPARISON = '{}/submit'.format(COMPARISONS)

COMPARISONS_PUBSUB_TOPIC = 'comparisons'
COMPARISONS_SUBSCRIPTION_ID = 'comparisons-sub'
COMPARISONS_ENDPOINT = 'https://us-central1-pubsub.googleapis.com/push'

COMPUTE_COMPARISON = '{}/comparison'.format(COMPUTE)

def compute_service_urlify(s):
    return COMPUTE_ENDPOINT + s

def main_service_urlify(s):
    return MAIN_ENDPOINT + s