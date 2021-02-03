import pytest

from cshift.core.compare.ks import KSComparison
from cshift.core.compare.lr import LRComparison
from cshift.core.compare.summary_stats import SummaryStatsComparison
from cshift.core.result.result import Result

from .test_compare.test_comparison import (
    ks_comparison,
    lr_comparison,
    summary_stats_comparison
)

SCOPE = 'package'

@pytest.fixture(scope=SCOPE)
def ks_result(ks_comparison: KSComparison) -> Result:
    return ks_comparison.compare()

@pytest.fixture(scope=SCOPE)
def lr_result(lr_comparison: LRComparison) -> Result:
    return lr_comparison.compare()

@pytest.fixture(scope=SCOPE)
def summary_stats_result(summary_stats_comparison: SummaryStatsComparison) -> Result:
    return summary_stats_comparison.compare()