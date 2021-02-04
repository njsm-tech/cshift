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
        wind_power
        )


def test_gen(normal_sep_ds, normal_unsep_ds):
    assert not SummaryStatsComparison.shift_detected(*normal_unsep_ds)
    assert not KSComparison.shift_detected(*normal_unsep_ds)
    assert not LRComparison.shift_detected(*normal_unsep_ds)

    assert SummaryStatsComparison.shift_detected(*normal_sep_ds)
    assert KSComparison.shift_detected(*normal_sep_ds)
    assert LRComparison.shift_detected(*normal_sep_ds)

def test_kgl(wind_power):
    cs = ComparisonSet.default()
    assert cs.shift_detected(*wind_power)
