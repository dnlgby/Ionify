from sqlalchemy import create_engine

from connections.connection import Connection


class MySQLConnection(Connection):
    """
    MySQLConnection is a concrete subclass of Connection that represents a connection to a MySQL database.

    Attributes:
    - _database: A string representing the name of the database.

    The following methods are implemented in this class:
    - from_config: A class method that creates an instance of MySQLConnection from a configuration dictionary.
    - connect: Opens the connection to the MySQL database.
    - disconnect: Closes the connection to the MySQL database.
    - check_health: Checks whether the connection to the MySQL database is healthy.
    - create_connection_string: Returns the connection string for connecting to the MySQL database.
    """

    def __init__(self, name, host, port, database, username, password, ssl_keyfile_path=None, ssl_certfile_path=None,
                 ssl_ca_certs=None, **addit_kwargs):
        super().__init__(name, host, port, username, password, ssl_keyfile_path, ssl_certfile_path, ssl_ca_certs,
                         **addit_kwargs)
        self._database = database

    @classmethod
    def from_config(cls, config):
        """
        Create an instance of MySQLConnection from a configuration dictionary.

        Args:
        - config: A dictionary containing the configuration parameters.

        Returns:
        - An instance of MySQLConnection.

        Raises:
        - MissingConfigurationKey: If any required configuration keys are missing.
        """
        # Validate configuration keys
        cls.validate_config_keys(config, ['name', 'host', 'port', 'database', 'username', 'password'])

        # Extract the keys specific to MySQL from the config
        return cls(
            config['name'], config['host'], config['port'], config['database'], config['username'], config['password'],
            config.get('ssl', {}).get('key'), config.get('ssl', {}).get('cert'), config.get('ssl', {}).get('ca')
        )

    def connect(self):
        """
        Open the connection to the MySQL database.
        """
        connection_string = self.create_connection_string()
        self._connection_engine = create_engine(connection_string, **self._addit_kwargs)

    def disconnect(self):
        """
        Close the connection to the MySQL database.
        """
        # SQLAlchemy engine doesn't require explicit disconnect. It's handled automatically.
        pass

    def check_health(self):
        """
        Check whether the connection to the MySQL database is healthy.

        Returns:
        - True if the connection is healthy, False otherwise.
        """
        try:
            self._connection_engine.execute("SELECT 1")
            return True
        except:
            return False

    def create_connection_string(self):
        """
        Create the connection string for connecting to the MySQL database.

        Returns:
        - The connection string.
        """
        return f"mysql+pymysql://{self._username}:{self._password}@{self._host}:{self._port}/{self._database}"
