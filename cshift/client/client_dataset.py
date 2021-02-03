from typing import List, Union

import sys

import numpy as np
import pandas as pd

from cshift.client_service_common import api_paths, utils
import cshift.client_service_common.config as csc_config
from cshift.dao.artifact import Artifact
from cshift.dao.artifact_gcs_path import ArtifactGcsPath
from cshift.proto import cshift_pb2 as pb2

from .client_object import ClientObject
from .client_config import ClientConfig

class ClientDataset(ClientObject):
    def __init__(self,
                 config: ClientConfig = None,
                 data: Union[np.ndarray, pd.DataFrame] = None,
                 feature_data: Union[np.ndarray, pd.DataFrame] = None,
                 label_data: Union[np.ndarray, pd.DataFrame] = None,
                 feature_cols: List[str] = None,
                 label_cols: List[str] = None,
                 ref: str = None,
                 name: str = None,
                 tags: List[str] = None,
                 version: str = None):
        super().__init__(config=config)
        self.name = name
        self.feature_cols = feature_cols
        self.label_cols = label_cols

        _path_ext = "{username}/{path_prefix}/{dataset_name}".format(
            username=self.config.username,
            path_prefix=csc_config.DATASETS_PATH_PREFIX,
            dataset_name=self.name)
        self.gcs_path = ArtifactGcsPath(
            bucket=csc_config.DATASETS_BUCKET,
            username=config.username,
            project=config.project,
            artifact_type=pb2.ArtifactType.DATASET_ARTIFACT,
            artifact_name=name,
            artifact_version=version)

        if (data is None) and (feature_data is not None and label_data is not None):
            data = pd.DataFrame(feature_data)
            data['labels'] = label_data
        self.data = data

        self._validate_xor(data, ref)
        self._check_size(data)

        self.is_data_literal = data is not None
        self.is_data_ref = ref is not None

        if self.is_data_literal:
            self.dataframe_parquet_bytes = Artifact.dataframe_to_parquet_bytes(data)
        else:
            self.dataframe_parquet_bytes = None

        artifact_spec = pb2.ArtifactSpec(
            name=name,
            artifact_type=pb2.ArtifactType.DATASET_ARTIFACT,
            gcs_path=self.gcs_path.to_message(),
            deserialized_type=pb2.ArtifactDeserializedType.PANDAS_DATAFRAME,
            serialization_format=pb2.ArtifactSerializationFormat.PARQUET
        )
        metadata = pb2.CShiftMetadata(
            name=name,
            id=utils.make_id(self),
            version=version)
        self.spec = pb2.DatasetSpec(
            metadata=metadata,
            tags=tags,
            artifact_spec=artifact_spec
        )

    def register(self):
        if self.is_data_literal:
            artifact = Artifact(self.spec.artifact_spec)
            artifact.upload(bytes=self.dataframe_parquet_bytes)
        return self.request_post(
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