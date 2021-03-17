from __future__ import annotations

from cshift.core.result.result import Result
from cshift.proto import cshift_pb2 as pb2

class ResultSet:
    def __init__(self, *results: Result):
        self.results = list(results)

    def add_result(self, result: Result) -> None:
        self.results.append(result)

    def get(self) -> None:
        for res in self.results:
            res.get()

    def record(self) -> None:
        for res in self.results:
            res.record()

    def to_message(self) -> pb2.ResultSetSpec:
        result_specs = []
        for res in self.results:
            result_specs.append(res.to_message())
        return pb2.ResultSetSpec(
            result_specs=result_specs)
