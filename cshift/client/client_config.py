from __future__ import annotations

from dataclasses import dataclass
from typing import Dict

import os
import simplejson as json

DEFAULT_CONFIG_PATH = '/Users/Nick/.cshift/cshift_config.json'
DEFAULT_USERNAME = 'njsm-tech'
DEFAULT_API_KEY = 'test-api-key'

@dataclass
class ClientConfig:
    username: str = None,
    api_key: str = None,
    project: str = None,
    path: str = DEFAULT_CONFIG_PATH

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
        path = path or cls.path
        with open(path, 'r') as f:
            js = json.load(f)
        return cls.from_dict(js)

    @classmethod
    def read_or_default(cls, path=None) -> ClientConfig:
        try:
            return cls.read(path=path)
        except:
            return cls(
                username=DEFAULT_USERNAME,
                api_key=DEFAULT_API_KEY,
                path=DEFAULT_CONFIG_PATH)

    def write(self, path=None) -> None:
        path = path or self.path
        with open(path, 'w') as f:
            json.dump(self.to_dict(), f)

    @staticmethod
    def cleanup():
        try:
            os.remove(DEFAULT_CONFIG_PATH)
        except OSError:
            pass