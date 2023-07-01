from abc import ABC, abstractmethod

from connections.sql_connection import SQLConnection
from datasources.datasource import DataSource


class SQLDataSource(DataSource, ABC):

    def __init__(self, connection: SQLConnection):
        super().__init__(connection)

    def register_model(self, model):
        """
        Register a user-defined model with the SQL database.

        Args:
        - model: A SQLAlchemy model class.

        Raises:
        - AssertionError: If the model is not a subclass of SQLAlchemy's Base.
        """
        self._connection.register_model(model)

    def create_all_user_defined_models(self):
        """
        Create tables for all user-defined models that have been registered with the SQL database.
        """
        self._connection.create_all_user_defined_models()

    def get_model(self, table_name):
        """
        Get the SQLAlchemy model for the given table name.

        Args:
        - table_name: The name of the table.

        Returns:
        - The SQLAlchemy model for the table, or None if no such table exists.
        """
        return self._connection.automap_base_model.classes.get(table_name)

    @abstractmethod
    def insert(self, data_entity_key: str, data: dict):
        pass

    @abstractmethod
    def update(self, data_entity_key: str, data_entity_id, data: dict):
        pass

    @abstractmethod
    def remove(self, data_entity_key: str, data_entity_id):
        pass

    @abstractmethod
    def query(self, query_string: str):
        pass
