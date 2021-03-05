from dataclasses import dataclass

@dataclass
class ClientConfig:
    username: str = None,
    api_key: str = None,
    application: str = None
