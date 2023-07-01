from sqlalchemy import create_engine

from connections.connection import Connection
from connections.sql_connection import SQLConnection


class PostgreConnection(SQLConnection):
    """
    PostgreConnection is a concrete subclass of Connection that represents a connection to a PostgreSQL database.

    Attributes:
    - _database: A string representing the name of the database.

    The following methods are implemented in this class:
    - from_config: A class method that creates an instance of PostgreConnection from a configuration dictionary.
    - connect: Opens the connection to the PostgreSQL database.
    - disconnect: Closes the connection to the PostgreSQL database.
    - check_health: Checks whether the connection to the PostgreSQL database is healthy.
    - create_connection_string: Returns the connection string for connecting to the PostgreSQL database.
    """

    class PostgreSQLConfigKeys(Connection.ConfigKeys):
        NAME = 'name'
        HOST = 'host'
        PORT = 'port'
        DATABASE = 'database'
        USERNAME = 'username'
        PASSWORD = 'password'
        SSL = 'ssl'
        SSL_KEY = 'key'
        SSL_CERT = 'cert'
        SSL_CA = 'ca'

        @classmethod
        def required_keys(cls):
            return [member.value for member in cls if
                    member.value not in [cls.SSL.value, cls.SSL_KEY.value, cls.SSL_CERT.value, cls.SSL_CA.value]]

    def __init__(self, name, host, port, database, username, password, ssl_keyfile_path=None, ssl_certfile_path=None,
                 ssl_ca_certs=None):
        super().__init__(name, host, port, database, username, password, ssl_keyfile_path, ssl_certfile_path,
                         ssl_ca_certs)

    @classmethod
    def from_dict(cls, config):
        """
        Create an instance of PostgreConnection from a configuration dictionary.

        Args:
        - config: A dictionary containing the configuration parameters.

        Returns:
        - An instance of PostgreConnection.

        Raises:
        - MissingConfigurationKey: If any required configuration keys are missing.
        """

        # Initiate config keys class
        required_config_keys = cls.PostgreSQLConfigKeys.required_keys()

        # Validate configuration keys
        cls.validate_dict_keys(config, required_config_keys)

        # Extract the keys specific to PostgreSQL from the config
        return cls(
            config[cls.PostgreSQLConfigKeys.NAME.value],
            config[cls.PostgreSQLConfigKeys.HOST.value],
            config[cls.PostgreSQLConfigKeys.PORT.value],
            config[cls.PostgreSQLConfigKeys.DATABASE.value],
            config[cls.PostgreSQLConfigKeys.USERNAME.value],
            config[cls.PostgreSQLConfigKeys.PASSWORD.value],
            config.get(cls.PostgreSQLConfigKeys.SSL.value, {}).get(cls.PostgreSQLConfigKeys.SSL.SSL_KEY.value),
            config.get(cls.PostgreSQLConfigKeys.SSL.value, {}).get(cls.PostgreSQLConfigKeys.SSL.SSL_CERT.value),
            config.get(cls.PostgreSQLConfigKeys.SSL.value, {}).get(cls.PostgreSQLConfigKeys.SSL.SSL_CA.value)
        )

    def _create_engine(self):
        """
        Create the SQLAlchemy engine for a PostgreSQL database connection, including SSL configuration if applicable.

        SSL Configuration:
        If SSL parameters are provided (ssl_certfile_path, ssl_keyfile_path, ssl_ca_certs),
        the connection will be established using SSL encryption. The arguments for SSL configuration are:
        - 'sslmode': 'require',
        - 'sslrootcert': self._ssl_ca_certs,
        - 'sslcert': self._ssl_certfile_path,
        - 'sslkey': self._ssl_keyfile_path,

        If SSL parameters are not provided, no SSL encryption will be used.

        Returns:
        - The SQLAlchemy engine object.

        Raises:
        - MissingConfigurationKey: If any required configuration keys are missing.
        """

        ssl_args = {}

        if self._ssl:
            ssl_args = {
                'sslmode': 'require',
                'sslrootcert': self._ssl_ca_certs,
                'sslcert': self._ssl_certfile_path,
                'sslkey': self._ssl_keyfile_path,
            }

        self._connection_engine = create_engine(self.create_connection_string(), connect_args=ssl_args)

    def create_connection_string(self):
        """
        Create the connection string for connecting to the PostgreSQL database.

        Returns:
        - The connection string.
        """
        return f"postgresql://{self._username}:{self._password}@{self._host}:{self._port}/{self._database}"
