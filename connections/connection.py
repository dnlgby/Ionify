from abc import ABC, abstractmethod

from connections.connections_exceptions.connection import MissingConfigurationKey


class Connection(ABC):
    def __init__(self, name, host, port, username, password, ssl_keyfile_path=None, ssl_certfile_path=None,
                 ssl_ca_certs=None):
        self._name = name
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._ssl_keyfile_path = ssl_keyfile_path
        self._ssl_certfile_path = ssl_certfile_path
        self._ssl_ca_certs = ssl_ca_certs
        self._connection_engine = None

    @property
    def name(self):
        return self._name

    @property
    def connection_engine(self):
        return self._connection_engine

    @classmethod
    @abstractmethod
    def from_config(cls, config):
        pass

    @classmethod
    def validate_config_keys(cls, config, required_keys):
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise MissingConfigurationKey(f"Missing required keys in the configuration: {missing_keys}")

    @abstractmethod
    def connect(self):
        pass

    @abstractmethod
    def disconnect(self):
        pass

    @abstractmethod
    def check_health(self):
        pass

    @abstractmethod
    def create_connection_string(self):
        pass

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self):
        self.disconnect()

    def __del__(self):
        self.disconnect()
