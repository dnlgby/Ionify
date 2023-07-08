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

    def get_new_session(self):
        """
        Get new SQLAlchemy session
        """
        return self._connection.get_new_session()

    @property
    def declarative_base_model(self):
        """Get the SQLAlchemy declarative base model class instance."""
        return self._connection.declarative_base_model

    @abstractmethod
    def insert(self, data_entity_key: str, data: dict):
        """
        Insert new data into the specified table.

        Args:
        - data_entity_key: The name of the table.
        - data: A dictionary containing the data to insert.
        """
        pass

    @abstractmethod
    def update(self, data_entity_key: str, data_entity_id, data: dict):
        """
        Update data in the specified table.

        Args:
        - data_entity_key: The name of the table.
        - data_entity_id: The ID of the record to update.
        - data: A dictionary containing the data to update.
        """
        pass

    @abstractmethod
    def remove(self, data_entity_key: str, data_entity_id):
        """
        Remove a record from the specified table.

        Args:
        - data_entity_key: The name of the table.
        - data_entity_id: The ID of the record to remove.
        """
        pass

    @abstractmethod
    def query(self, query_string: str):
        """
        Execute a raw SQL query.

        Args:
        - query_string: The SQL query to execute.
        """
        pass

    @abstractmethod
    def find_by_id(self, data_entity_key: str, data_entity_id):
        """
        Get a record by its unique ID.

        Args:
        - data_entity_key: The name of the table.
        - data_entity_id: The ID of the record to retrieve.
        """
        pass

    @abstractmethod
    def find_all(self, data_entity_key: str):
        """
        Get all records from the specified table.

        Args:
        - data_entity_key: The name of the table.
        """
        pass

    @abstractmethod
    def find_by_field(self, data_entity_key: str, field_name: str, field_value):
        """
        Get records that match a specific field value.

        Args:
        - data_entity_key: The name of the table.
        - field_name: The name of the field to filter by.
        - field_value: The value to match.
        """
        pass

    @abstractmethod
    def count(self, data_entity_key: str):
        """
        Count the number of records in the specified table.

        Args:
        - data_entity_key: The name of the table.
        """
        pass

    @abstractmethod
    def exists(self, data_entity_key: str, data_entity_id):
        """
        Check if a record exists.

        Args:
        - data_entity_key: The name of the table.
        - data_entity_id: The ID of the record to check.
        """
        pass

    @abstractmethod
    def inner_join(self, primary_table: str, secondary_table: str, on_field: str):
        """
        Perform an inner join between two tables.

        Args:
        - primary_table: The name of the first table.
        - secondary_table: The name of the second table.
        - on_field: The field to join on.
        """
        pass

    @abstractmethod
    def left_join(self, primary_table: str, secondary_table: str, on_field: str):
        """
        Perform a left join between two tables.

        Args:
        - primary_table: The name of the first table.
        - secondary_table: The name of the second table.
        - on_field: The field to join on.
        """
        pass

    @abstractmethod
    def right_join(self, primary_table: str, secondary_table: str, on_field: str):
        """
        Perform a right join between two tables.

        Args:
        - primary_table: The name of the first table.
        - secondary_table: The name of the second table.
        - on_field: The field to join on.
        """
        pass
