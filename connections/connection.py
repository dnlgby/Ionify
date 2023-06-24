from abc import ABC, abstractmethod

from connections.connections_exceptions.connection import MissingConfigurationKey


class Connection(ABC):
    """
    Connection is an abstract base class that represents a generic connection to a database or other data sources.
    Specific types of connections should inherit from this class and implement the abstract methods.

    Attributes:
    - _name: A string that represents the name of the connection.
    - _host: A string that represents the host of the data source.
    - _port: An integer that represents the port number of the data source.
    - _username: A string that represents the username used for authentication.
    - _password: A string that represents the password used for authentication.
    - _ssl_keyfile_path: (Optional) A string that represents the path to the SSL key file.
    - _ssl_certfile_path: (Optional) A string that represents the path to the SSL certificate file.
    - _ssl_ca_certs: (Optional) A string that represents the path to the SSL CA certificate file.
    - _connection_engine: The object responsible for maintaining the actual connection. Specific to the child class's implementation.

    The following methods must be implemented in any child class:
    - from_config: A class method that creates an instance of the connection from a configuration dictionary.
    - connect: Opens the connection.
    - disconnect: Closes the connection.
    - check_health: Checks whether the connection is healthy.
    - create_connection_string: Returns a connection string specific to the type of connection.

    The class also includes __enter__ and __exit__ methods to allow its instances to be used with Python's with statement.
    """

    def __init__(self, name, host, port, username, password, ssl_keyfile_path=None, ssl_certfile_path=None,
                 ssl_ca_certs=None, **addit_kwargs):
        self._name = name
        self._host = host
        self._port = port
        self._username = username
        self._password = password
        self._ssl_keyfile_path = ssl_keyfile_path
        self._ssl_certfile_path = ssl_certfile_path
        self._ssl_ca_certs = ssl_ca_certs
        self._connection_engine = None
        self._addit_kwargs = addit_kwargs


    @property
    def name(self):
        """Get the name of the connection."""
        return self._name

    @property
    def connection_engine(self):
        """Get the connection engine."""
        return self._connection_engine

    @classmethod
    def from_config(cls, config):
        """
        Create an instance of the connection from a configuration dictionary.

        Args:
        - config: A dictionary containing the configuration parameters.

        Returns:
        - An instance of the connection.

        Raises:
        - MissingConfigurationKey: If any required configuration keys are missing.
        """
        pass

    @classmethod
    def validate_config_keys(cls, config, required_keys):
        """
        Validate the presence of required keys in the configuration dictionary.

        Args:
        - config: A dictionary containing the configuration parameters.
        - required_keys: A list of required keys.

        Raises:
        - MissingConfigurationKey: If any required keys are missing.
        """
        missing_keys = [key for key in required_keys if key not in config]
        if missing_keys:
            raise MissingConfigurationKey(f"Missing required keys in the configuration: {missing_keys}")

    @abstractmethod
    def connect(self):
        """Open the connection."""
        pass

    @abstractmethod
    def disconnect(self):
        """Close the connection."""
        pass

    @abstractmethod
    def check_health(self):
        """
        Check whether the connection is healthy.

        Returns:
        - True if the connection is healthy, False otherwise.
        """
        pass

    @abstractmethod
    def create_connection_string(self):
        """
        Create a connection string specific to the type of connection.

        Returns:
        - The connection string.
        """
        pass

    def __enter__(self):
        """Open the connection when entering a with statement."""
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Close the connection when exiting a with statement."""
        self.disconnect()

    def __del__(self):
        """Close the connection when the instance is deleted."""
        self.disconnect()
