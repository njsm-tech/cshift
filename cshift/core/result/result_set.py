from __future__ import annotations

from cshift.core.result.base_result import BaseResult
from cshift.core.result.result import Result

class ResultSet(BaseResult):
    def __init__(self, *results: Result, **kwargs):
        super().__init__(**kwargs)
        self.results = list(results)

    def add_result(self, result: Result) -> None:
        self.results.append(result)

    def get(self) -> None:
        for res in self.results:
            res.get()

    def record(self) -> None:
        for res in self.results:
            res.record()