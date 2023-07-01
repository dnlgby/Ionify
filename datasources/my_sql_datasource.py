from connections import MySQLConnection
from datasources.datasource import DataSource


class MySqlDataSource(DataSource):

    def __init__(self, connection: MySQLConnection):
        super().__init__(connection)

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    def insert(self, data_entity_key: str, data: dict):
        pass

    def update(self, data_entity_key: str, data_entity_id, data: dict):
        pass

    def remove(self, data_entity_key: str, data_entity_id, data: dict):
        pass

    def query(self, query_string: str):
        pass
