ROOT = '/'

PROJECT = 'cshift'

COMPARISONS = 'comparisons'
COMPUTE = 'compute'
DATASETS = 'datasets'
MODELS = 'models'
REGISTER_DATASET = '{}/register'.format(DATASETS)
REGISTER_MODEL = '{}/register'.format(MODELS)
SUBMIT_COMPARISON = '{}/submit'.format(COMPARISONS)

COMPARISONS_PUBSUB_TOPIC = 'comparisons'
COMPARISONS_SUBSCRIPTION_ID = 'comparisons-sub'
COMPARISONS_ENDPOINT = 'https://us-central1-pubsub.googleapis.com/push'

COMPUTE_COMPARISON = '{}/comparison'.format(COMPUTE)