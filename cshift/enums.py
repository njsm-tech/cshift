class SummaryStats:
    NULL_RATE = 'null_rate'
    MEAN = 'mean'
    MEDIAN = 'median'
    VAR = 'var'
    QUANTILES = 'quantiles'

class DatastoreEntityKind:
    DATASET = 'Dataset'
    MODEL = 'Model'
    COMPARISON = 'Comparison'
    RESULT = 'Result'

class ResponseCode:
    SUCCESS = 'success'
    FAILED = 'failed'
    TASK_QUEUED = 'task_queued'