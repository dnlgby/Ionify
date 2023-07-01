from abc import ABC, abstractmethod

from connections.connection import Connection


class DataSource(ABC):

    def __init__(self, connection: Connection):
        self._connection = connection
        self._connection_engine = connection.connection_engine

    @property
    def connection_engine(self):
        return self._connection_engine

    def connect(self):
        self._connection.connect()

    def disconnect(self):
        self._connection.disconnect()

    def check_health(self):
        return self._connection.check_health()

    @abstractmethod
    def insert(self, data_entity_key: str, data: dict):
        pass

    @abstractmethod
    def update(self, data_entity_key: str, data_entity_id, data: dict):
        pass

    @abstractmethod
    def remove(self, data_entity_key: str, data_entity_id, data: dict):
        pass

    @abstractmethod
    def query(self, query_string: str):
        pass
