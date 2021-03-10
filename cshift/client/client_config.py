from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

import os
import simplejson as json

CONFIG_TMP_PATH = './.cshift_config.json'

@dataclass
class ClientConfig:
    username: str = None,
    api_key: str = None

    @property
    def gcs_prefix(self) -> str:
        return self.username

    def to_dict(self) -> Dict:
        return {
            'username': self.username,
            'api_key': self.api_key,
            'gcs_prefix': self.gcs_prefix
        }

    @classmethod
    def from_dict(cls, d: Dict) -> ClientConfig:
        return cls(
            username=d.get('username'),
            api_key=d.get('api_key'))

    @classmethod
    def read(cls, path=None) -> ClientConfig:
        path = path or CONFIG_TMP_PATH
        with open(path, 'r') as f:
            js = json.load(f)
        return cls.from_dict(js)

    def write(self, path=None) -> None:
        path = path or CONFIG_TMP_PATH
        with open(path, 'w') as f:
            json.dump(self.to_dict(), f)

    @staticmethod
    def cleanup():
        try:
            os.remove(CONFIG_TMP_PATH)
        except OSError:
            pass