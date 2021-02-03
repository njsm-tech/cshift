from typing import Union

from abc import ABC, ABCMeta, abstractmethod

from cshift.core.result.result import Result
from cshift.core.result.result_set import ResultSet
from cshift.proto import cshift_pb2 as pb2

class ComparisonInterface(ABC):
    __metaclass__ = ABCMeta

    @abstractmethod
    def compare(self) -> Union[Result, ResultSet]:
        """
        Checks for distributional shift between datasets using the
            comparison specified by the subclass.
        Returns pandas dataframe showing the computed distributional
            difference between the datasets, using the metrics
            specified by each subclass.

        Implemented by subclasses.
        """
        raise NotImplementedError()

    @classmethod
    @abstractmethod
    def make_spec(cls, *args, **kwargs) -> Union[
        pb2.ComparisonSpec,
        pb2.ComparisonSetSpec
    ]:
        raise NotImplementedError()

    @abstractmethod
    def shift_detected(self) -> bool:
        """
        Checks for distributional shift between datasets using the
            comparison specified by the subclass.
        Returns bool: True if shift is detected, False if shift is
            not detected.
        This method can be considered to be a reduction of 'compare'.

        Implemented by subclasses.
        """
        raise NotImplementedError()