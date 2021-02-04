from typing import List

import numpy as np
import pandas as pd
import scipy.stats as ss

from cshift.core.dataset import Dataset
from cshift.core.compare.summary_stats import SummaryStatsComparison
from cshift.core.compare.ks import KSComparison
from cshift.core.compare.lr import LRComparison
from cshift.core.compare.comparison_set import ComparisonSet

from random_data_configs import (
        normal_sep_ds,
        normal_unsep_ds
        )
from kaggle_data_configs import (
        drought, 
        wind_power
        )


def test_gen(normal_sep_ds, normal_unsep_ds):
    cs = ComparisonSet.default()
    assert not cs.shift_detected(*normal_unsep_ds)
    assert cs.shift_detected(*normal_sep_ds)

def test_kgl(drought, wind_power):
    cs = ComparisonSet.default()
    assert cs.shift_detected(*wind_power)
    assert cs.shift_detected(*drought)
