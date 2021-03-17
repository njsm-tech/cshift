from cshift.core.compare.comparison_set import ComparisonSet

from .random_data_configs import (
        normal_sep_ds,
        normal_unsep_ds
        )
from .kaggle_data_configs import (
        drought, 
        wind_power
        )

def test_gen(normal_sep_ds, normal_unsep_ds):
    cs = ComparisonSet.default(datasets=normal_unsep_ds)
    assert not cs.shift_detected()
    cs = ComparisonSet.default(datasets=normal_sep_ds)
    assert cs.shift_detected()

def test_kgl(drought, wind_power):
    cs = ComparisonSet.default(datasets=wind_power)
    assert cs.shift_detected()
    cs = ComparisonSet.default(datasets=drought)
    assert cs.shift_detected()
