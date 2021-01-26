from typing import List

import numpy as np
import scipy.stats as ss

from cshift.core.dataset import Dataset
from cshift.core.compare.summary_stats import SummaryStatsComparison

from random_data_configs import (
        normal_sep_ds1,
        normal_unsep_ds1
        )

def test_main(normal_sep_ds1, normal_unsep_ds1):
    assert not SummaryStatsComparison.datasets_same(normal_sep_ds1, normal_unsep_ds1)
