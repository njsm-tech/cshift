from typing import Dict

class FancyEnum:
    @classmethod
    def get_key_value_mapping(cls) -> Dict:
        return {k: v for k, v in cls.__dict__.items() if cls._is_enum_attr(k)}

    @classmethod
    def get_value_key_mapping(cls) -> Dict:
        return {v: k for k, v in cls.__dict__.items() if cls._is_enum_attr(k)}

    @classmethod
    def clsinit(cls) -> None:
        cls._KEY_VALUE_MAPPING = cls.get_key_value_mapping()
        cls._VALUE_KEY_MAPPING = cls.get_value_key_mapping()

    @staticmethod
    def _is_enum_attr(s) -> bool:
        if (not s.startswith('_')) and s.isupper():
            return True
        return False

    @classmethod
    def from_value(cls, value: str):
        if not hasattr(cls, '_VALUE_KEY_MAPPING'):
            cls.clsinit()
        if value not in cls._VALUE_KEY_MAPPING:
            raise ValueError("value %s not found" % value)
        key_name = cls._VALUE_KEY_MAPPING[value]
        return getattr(cls, key_name)

    @classmethod
    def get_all(cls):
        '''Get all keys'''
        if not hasattr(cls, '_KEY_VALUE_MAPPING'):
            cls.clsinit()
        return list(cls._KEY_VALUE_MAPPING.keys())

    @classmethod
    def get_all_values(cls):
        '''Get all values'''
        if not hasattr(cls, '_KEY_VALUE_MAPPING'):
            cls.clsinit()
        return list(cls._KEY_VALUE_MAPPING.values())

class SummaryStats(FancyEnum):
    NULL_RATE = 'null_rate'
    MEAN = 'mean'
    MEDIAN = 'median'
    VAR = 'var'
    QUANTILES = 'quantiles'

class ComparisonType(FancyEnum):
    SUMMARY_STATS = 'summary_stats'
    KS = 'ks'
    LR = 'lr'
    # COVARIANCE_MATRIX = 'covariance_matrix'

class DatastoreEntityKind(FancyEnum):
    ARTIFACT = 'Artifact'
    DATASET = 'Dataset'
    MODEL = 'Model'
    COMPARISON = 'Comparison'
    RESULT = 'Result'

class JobStatus(FancyEnum):
    QUEUED = 'queued'
    STARTED = 'started'
    COMPLETED = 'completed'