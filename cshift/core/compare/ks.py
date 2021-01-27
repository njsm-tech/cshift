from typing import List

import numpy as np
import pandas as pd

from .comparison import Comparison
from ..dataset import Dataset

class KSComparison(Comparison):
    @classmethod
    def compare(cls, *datasets):
        cls.validate_datasets(*datasets)

