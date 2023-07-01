from sqlalchemy.orm import Session

from datasources.sql_datasource import SQLDataSource


class MySqlDataSource(SQLDataSource):
    """
    MySqlDataSource is a concrete subclass of SQLDataSource that interfaces with a MySQL database.

    This class is designed to work with SQLAlchemy ORM, which allows high-level and Pythonic manipulation of SQL databases.

    Methods:
    - insert: Inserts a new record into a table in the MySQL database.
    - update: Updates an existing record in a table in the MySQL database.
    - remove: Deletes an existing record from a table in the MySQL database.
    - query: Executes a SQL query against the MySQL database.
    """

    def __init__(self, connection):
        super().__init__(connection)


    def insert(self, data_entity_key: str, data: dict):
        """
        Insert a new record into a table in the MySQL database.

        Args:
        - data_entity_key: The name of the table.
        - data: The record data as a dictionary.

        Returns:
        - The primary key of the inserted record.
        """
        session = self._connection.get_new_session()

        try:
            # Create an instance of the mapped class
            instance = self.get_model(data_entity_key)(**data)
            # Add the new instance to the session
            session.add(instance)
            # Commit the transaction
            session.commit()
            return instance.id
        finally:
            session.close()

    def update(self, data_entity_key: str, data_entity_id, data: dict):
        """
        Update an existing record in a table in the MySQL database.

        Args:
        - data_entity_key: The name of the table.
        - data_entity_id: The primary key of the record.
        - data: The new record data as a dictionary.
        """
        session = self._connection.get_new_session()

        try:
            # Query for the existing record
            instance = session.query(self.get_model(data_entity_key)).get(data_entity_id)
            if instance is None:
                return False

            # Update the record with the new data
            for key, value in data.items():
                setattr(instance, key, value)

            # Commit the transaction
            session.commit()
            return True
        finally:
            session.close()

    def remove(self, data_entity_key: str, data_entity_id):
        """
        Delete an existing record from a table in the MySQL database.

        Args:
        - data_entity_key: The name of the table.
        - data_entity_id: The primary key of the record.
        """
        session = self._connection.get_new_session()

        try:
            # Query for the existing record
            instance = session.query(self.get_model(data_entity_key)).get(data_entity_id)
            if instance is None:
                return False

            # Delete the record
            session.delete(instance)

            # Commit the transaction
            session.commit()
            return True
        finally:
            session.close()

    def query(self, query_string: str):
        """
        Execute a SQL query against the MySQL database.

        Args:
        - query_string: The SQL query string.

        Returns:
        - The result of the query.
        """
        result = self._connection.connection_engine.execute(query_string)
        return result.fetchall()
