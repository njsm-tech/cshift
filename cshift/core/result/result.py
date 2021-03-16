from __future__ import annotations

import pandas as pd

from cshift.dao.artifact import Artifact
from cshift.proto import cshift_pb2 as pb2

from .base_result import BaseResult

class Result(BaseResult):
    def __init__(self,
                 df: pd.DataFrame = None,
                 **kwargs):
        super().__init__(**kwargs)
        self.df = df

    def get(self) -> None:
        self.df = self.artifact.download()

    def record(self) -> None:
        bytes = Artifact.dataframe_to_parquet_bytes(self.df)
        self.artifact.upload(bytes=bytes)

    def to_message(self) -> pb2.ResultSpec:
        return pb2.ResultSpec(
            comparison_pipeline_spec=self.comparison_pipeline_spec,
            artifact_spec=self.artifact.spec)