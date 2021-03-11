from cshift.proto import cshift_pb2 as pb2

from . import ks, lr, summary_stats

def comparison_from_enum(comparison_type: pb2.ComparisonType):
    if comparison_type == pb2.ComparisonType.SUMMARY_STATS:
        return summary_stats.SummaryStatsComparison
    elif comparison_type == pb2.ComparisonType.KS:
        return ks.KSComparison
    elif comparison_type == pb2.ComparisonType.LR:
        return lr.LRComparison