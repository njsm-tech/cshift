import pytest

from typing import List

from ..random_data_configs import normal_sep_ds

from cshift.core.compare.ks import KSComparison
from cshift.core.compare.lr import LRComparison
from cshift.core.compare.summary_stats import SummaryStatsComparison
from cshift.core.dataset import Dataset

SCOPE = 'package'

@pytest.fixture(scope=SCOPE)
def ks_comparison(normal_sep_ds: List[Dataset]) -> KSComparison:
    return KSComparison(
        *normal_sep_ds,
        groupby_fields=[],
        index_fields=[])

@pytest.fixture(scope=SCOPE)
def lr_comparison(normal_sep_ds: List[Dataset]) -> LRComparison:
    return LRComparison(
        *normal_sep_ds,
        groupby_fields=[],
        index_fields=[])

@pytest.fixture(scope=SCOPE)
def summary_stats_comparison(normal_sep_ds: List[Dataset]) -> SummaryStatsComparison:
    return SummaryStatsComparison(
        *normal_sep_ds,
        groupby_fields=[],
        index_fields=[])

def test_compare(
        ks_comparison: KSComparison,
        lr_comparison: LRComparison,
        summary_stats_comparison: SummaryStatsComparison):
    assert ks_comparison.shift_detected()
    assert lr_comparison.shift_detected()
    assert summary_stats_comparison.shift_detected()