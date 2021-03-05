from typing import Union

import numpy as np
import pandas as pd

class ClientDatasetBuilder:
    pass

class ClientDatasetConfig:
    pass

class ClientDataset:
    def __init__(self, arr: Union[np.ndarray, pd.DataFrame]):
        self.arr = arr

    def register