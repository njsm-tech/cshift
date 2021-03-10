from typing import List, Union

from io import BytesIO
import re
import sys

import numpy as np
import pandas as pd

from cshift.proto import cshift_pb2 as pb2
from cshift.client_service_common import api_paths
import cshift.client_service_common.config as csc_config

from .client_object import ClientObject

class ClientDataset(ClientObject):
    def __init__(self,
                 data: Union[np.ndarray, pd.DataFrame] = None,
                 features: Union[np.ndarray, pd.DataFrame] = None,
                 labels: Union[np.ndarray, pd.DataFrame] = None,
                 ref: str = None,
                 storage_location_type: str = 'in_memory',
                 name: str = None,
                 tags: List[str] = None):
        if (data is None) and (features is not None and labels is not None):
            data = pd.DataFrame(features)
            print(data, labels)
            data['labels'] = labels

        self._validate_xor(data, ref)
        self._check_size(data)

        is_data_literal = data is not None
        is_data_ref = ref is not None

        if storage_location_type is None:
            if is_data_literal:
                storage_location_type = pb2.StorageLocationType.IN_MEMORY
            elif is_data_ref:
                if re.match(r'^gs://|^http', ref):
                    storage_location_type = pb2.StorageLocationType.REMOTE
                else:
                    storage_location_type = pb2.StorageLocationType.LOCAL

        elif storage_location_type == 'in_memory':
            storage_location_type = pb2.StorageLocationType.IN_MEMORY
        elif storage_location_type == 'local':
            storage_location_type = pb2.StorageLocationType.LOCAL
        elif storage_location_type == 'remote':
            storage_location_type = pb2.StorageLocationType.REMOTE
        else:
            raise ValueError(
                "Unrecognized storage_location_type %s" % storage_location_type)

        self.spec = pb2.DatasetSpec(
            name=name,
            dataframe_parquet_bytes=self.dataframe_to_parquet_bytes(data),
            ref=ref,
            is_data_literal=is_data_literal,
            is_data_ref=is_data_ref,
            storage_location_type=storage_location_type,
            tags=tags
        )

    def dataframe_to_parquet_bytes(self, df) -> bytes:
        buffer = BytesIO()
        df.to_parquet(buffer)
        return buffer.getvalue()

    def register(self):
        return self._post(
            url=api_paths.REGISTER_DATASET,
            spec=self.spec)

    def _data_to_spec(self, data):
        return pb2.DatasetSpec.PandasDataFrameSpec(
            index=data.index,
            columns=data.columns,
            data=data.values)

    def _check_size(self, data):
        if sys.getsizeof(data) > csc_config.CLIENT_MAX_DATA_SIZE_BYTES:
            raise ValueError("Data is too large to submit directly. "
                "Please submit a reference to a remote file.")

    def _validate_xor(self, data, ref):
        if data is not None and ref is not None:
            raise ValueError("Only one of 'data' and 'ref' can be supplied")
        if data is None and ref is None:
            raise ValueError("Please supply dataset or reference")