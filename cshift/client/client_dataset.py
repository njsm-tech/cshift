from typing import List, Union

from io import BytesIO
import sys

import numpy as np
import pandas as pd

from cshift.client_service_common import api_paths
import cshift.client_service_common.config as csc_config
from cshift.dao.artifact_dao import ArtifactDao
from cshift.dao.gcs_path import GcsPath
from cshift.proto import cshift_pb2 as pb2

from .client_object import ClientObject
from .client_config import ClientConfig

class ClientDataset(ClientObject):
    def __init__(self,
                 data: Union[np.ndarray, pd.DataFrame] = None,
                 features: Union[np.ndarray, pd.DataFrame] = None,
                 labels: Union[np.ndarray, pd.DataFrame] = None,
                 ref: str = None,
                 name: str = None,
                 tags: List[str] = None):
        self.config = ClientConfig.read()
        self.name = name

        _path_ext = "{username}/{dataset_name}".format(
            username=self.config.username,
            dataset_name=self.name)
        self.gcs_path = GcsPath(
            bucket=csc_config.DATASETS_BUCKET,
            path_ext=_path_ext)

        if (data is None) and (features is not None and labels is not None):
            data = pd.DataFrame(features)
            print(data, labels)
            data['labels'] = labels

        self._validate_xor(data, ref)
        self._check_size(data)

        self.is_data_literal = data is not None
        self.is_data_ref = ref is not None

        if self.is_data_literal:
            self.dataframe_parquet_bytes = self.dataframe_to_parquet_bytes(data)
        else:
            self.dataframe_parquet_bytes = None

        artifact_spec = pb2.ArtifactSpec(
            name=name,
            artifact_type=pb2.ArtifactType.DATASET,
            gcs_path=self.gcs_path.to_message()
        )
        self.spec = pb2.DatasetSpec(
            name=name,
            tags=tags,
            artifact_spec=artifact_spec
        )

    def dataframe_to_parquet_bytes(self, df: pd.DataFrame) -> bytes:
        buffer = BytesIO()
        df.columns = [str(c) for c in df.columns]
        df.to_parquet(buffer)
        return buffer.getvalue()

    def register(self):
        if self.is_data_literal:
            dao = ArtifactDao(self.spec.artifact_spec)
            dao.upload(bytes=self.dataframe_parquet_bytes)
        return self._post(
            url=api_paths.REGISTER_DATASET,
            spec=self.spec)

    def _check_size(self, data):
        if sys.getsizeof(data) > csc_config.CLIENT_MAX_DATA_SIZE_BYTES:
            raise ValueError("Data is too large to submit directly. "
                "Please submit a reference to a remote file.")

    def _validate_xor(self, data, ref):
        if data is not None and ref is not None:
            raise ValueError("Only one of 'data' and 'ref' can be supplied")
        if data is None and ref is None:
            raise ValueError("Please supply dataset or reference")