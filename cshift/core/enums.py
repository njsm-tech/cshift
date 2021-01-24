from enum import Enum

class ColumnType(Enum):
    FLOAT = 1
    INT = 2
    STR = 3

class FeatureType(Enum):
    NUM = 1
    ORD = 2
    CAT = 3
