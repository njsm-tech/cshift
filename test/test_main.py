from typing import List

import numpy as np
import scipy.stats as ss

from cshift.core.dataset import Dataset
from cshift.core.compare.summary_stats import SummaryStatsComparison

from random_data_configs import (
        normal_sep_ds1,
        normal_unsep_ds1
        )

ATOL = 1e-2

def test_main(normal_sep_ds1, normal_unsep_ds1):
    diff = SummaryStatsComparison.datasets_same(
            normal_sep_ds1, normal_unsep_ds1)
    zeros = np.zeros_like(diff)
    assert not np.all(np.isclose(diff, zeros, atol=ATOL))
