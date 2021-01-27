from typing import List

import numpy as np
import pandas as pd
import scipy.stats as ss

from cshift.core.dataset import Dataset
from cshift.core.compare.summary_stats import SummaryStatsComparison

from random_data_configs import (
        normal_sep_ds,
        normal_unsep_ds
        )

ATOL = 1e-1

def datasets_same(ds1: Dataset, ds2: Dataset):
    diff = SummaryStatsComparison.compare(ds1, ds2)
    return _diff_zero(diff)

def _diff_zero(diff: pd.DataFrame):
    zeros = np.zeros_like(diff.values)
    return np.all(np.isclose(diff.values, zeros, atol=ATOL))

def test_main(normal_sep_ds, normal_unsep_ds):
    assert datasets_same(*normal_unsep_ds)
    assert not datasets_same(*normal_sep_ds)
