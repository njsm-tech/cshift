from typing import List, Union

import sys

import numpy as np
import pandas as pd

from cshift.proto import cshift_pb2 as pb2
import cshift.client_service_common.config as csc_config

class ClientDataset:
    def __init__(self,
                 data: Union[np.ndarray, pd.DataFrame] = None,
                 ref: str = None,
                 storage_location_type: str = None,
                 name: str = None,
                 tags: List[str] = None):
        self._validate_xor(data, ref)
        self._check_size(data)

        is_data_literal = data is not None
        is_data_ref = ref is not None
        if storage_location_type == 'local':
            storage_location_type = pb2.StorageLocationType.LOCAL
        elif storage_location_type == 'remote':
            storage_location_type = pb2.StorageLocationType.REMOTE
        else:
            raise ValueError(
                "Unrecognized storage_location_type %s" % storage_location_type)

        self.dataset_spec = pb2.DatasetSpec(
            name=name,
            data=data,
            ref=ref,
            is_data_literal=is_data_literal,
            is_data_ref=is_data_ref,
            storage_location_type=storage_location_type,
            tags=tags
        )

    def _check_size(self, data):
        if sys.getsizeof(data) > csc_config.CLIENT_MAX_DATA_SIZE_BYTES:
            raise ValueError("Data is too large to submit directly. "
                "Please submit a reference to a remote file.")

    def _validate_xor(self, data, ref):
        if data is not None and ref is not None:
            raise ValueError("Only one of 'data' and 'ref' can be supplied")
        if data is None and ref is None:
            raise ValueError("Please supply dataset or reference")