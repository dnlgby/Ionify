from abc import ABC, abstractmethod

from connections.connection import Connection


class DataSource(ABC):
    """
    DataSource is an abstract base class that defines a common interface for data sources. This class handles the
    connection to the database and provides abstract methods for data manipulation operations.

    Subclasses of DataSource should implement the abstract methods to provide concrete behavior for data manipulation.

    Attributes:
    - _connection: A Connection object that manages the connection to the database.
    - _connection_engine: The actual connection engine that is used to interact with the database.

    The following methods are implemented in this class:
    - connect: Opens the connection to the database.
    - disconnect: Closes the connection to the database.
    - check_health: Checks whether the connection to the database is healthy.

    The following methods are abstract and must be implemented in subclasses:
    - insert: Inserts data into the database.
    - update: Updates data in the database.
    - remove: Removes data from the database.
    - query: Executes a query against the database and returns the result.
    """

    def __init__(self, connection: Connection):
        """
        Construct a new DataSource instance.

        Args:
        - connection: A Connection object that manages the connection to the database.
        """
        self._connection = connection
        self._connection_engine = connection.connection_engine

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()

    @property
    def connection_engine(self):
        """
        Get the connection engine that is used to interact with the database.

        Returns:
        - The connection engine.
        """
        return self._connection_engine

    def connect(self):
        """
        Open the connection to the database.
        """
        self._connection.connect()

    def disconnect(self):
        """
        Close the connection to the database.
        """
        self._connection.disconnect()

    def check_health(self):
        """
        Check whether the connection to the database is healthy.

        Returns:
        - True if the connection is healthy, False otherwise.
        """
        return self._connection.check_health()

    @abstractmethod
    def insert(self, data_entity_key: str, data: dict):
        """
        Insert data into the database.

        Args:
        - data_entity_key: A string representing the key of the data entity.
        - data: A dictionary containing the data to insert.

        This is an abstract method and must be implemented in subclasses.
        """
        pass

    @abstractmethod
    def update(self, data_entity_key: str, data_entity_id, data: dict):
        """
        Update data in the database.

        Args:
        - data_entity_key: A string representing the key of the data entity.
        - data_entity_id: The id of the data entity to update.
        - data: A dictionary containing the new data.

        This is an abstract method and must be implemented in subclasses.
        """
        pass

    @abstractmethod
    def remove(self, data_entity_key: str, data_entity_id):
        """
        Remove data from the database.

        Args:
        - data_entity_key: A string representing the key of the data entity.
        - data_entity_id: The id of the data entity to remove.

        This is an abstract method and must be implemented in subclasses.
        """
        pass

    @abstractmethod
    def query(self, query_string: str):
        """
        Execute a query against the database and return the result.

        Args:
        - query_string: A string containing the SQL query to execute.

        Returns:
        - The result of the query.

        This is an abstract method and must be implemented in subclasses.
        """
        pass
