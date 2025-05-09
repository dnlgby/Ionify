from abc import ABC

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

    def __enter__(self):
        """Open the datasource connection when entering a with statement."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the datasource connection when exiting a with statement."""
        self.disconnect()

    def __del__(self):
        """Close the datasource connection when the instance is deleted."""
        self.disconnect()
