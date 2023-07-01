from pymongo import MongoClient
from pymongo.errors import ServerSelectionTimeoutError

from connections.connection import Connection


class MongoDBConnection(Connection):
    """
    MongoDBConnection is a concrete subclass of Connection that represents a connection to a MongoDB database.

    Attributes:
    - _connection_engine: A MongoClient object representing the connection to the MongoDB database.

    The following methods are implemented in this class:
    - from_config: A class method that creates an instance of MongoDBConnection from a configuration dictionary.
    - connect: Opens the connection to the MongoDB database.
    - disconnect: Closes the connection to the MongoDB database.
    - check_health: Checks whether the connection to the MongoDB database is healthy.
    - create_connection_string: Returns the connection string for connecting to the MongoDB database.
    """

    class MongoDBConfigKeys(Connection.ConfigKeys):
        NAME = 'name'
        HOST = 'host'
        PORT = 'port'
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

    def __init__(self, name, host, port, username, password, ssl_keyfile_path=None, ssl_certfile_path=None,
                 ssl_ca_certs=None):
        super().__init__(name, host, port, username, password, ssl_keyfile_path, ssl_certfile_path, ssl_ca_certs)

    @classmethod
    def from_dict(cls, config):
        """
        Create an instance of MongoDBConnection from a configuration dictionary.

        Args:
        - config: A dictionary containing the configuration parameters.

        Returns:
        - An instance of MongoDBConnection.

        Raises:
        - MissingConfigurationKey: If any required configuration keys are missing.
        """
        required_config_keys = cls.MongoDBConfigKeys.required_keys()
        cls.validate_dict_keys(config, required_config_keys)

        return cls(
            config[cls.MongoDBConfigKeys.NAME.value],
            config[cls.MongoDBConfigKeys.HOST.value],
            config[cls.MongoDBConfigKeys.PORT.value],
            config[cls.MongoDBConfigKeys.USERNAME.value],
            config[cls.MongoDBConfigKeys.PASSWORD.value],
            config.get(cls.MongoDBConfigKeys.SSL.value, {}).get(cls.MongoDBConfigKeys.SSL.SSL_KEY.value),
            config.get(cls.MongoDBConfigKeys.SSL.value, {}).get(cls.MongoDBConfigKeys.SSL.SSL_CERT.value),
            config.get(cls.MongoDBConfigKeys.SSL.value, {}).get(cls.MongoDBConfigKeys.SSL.SSL_CA.value)
        )

    def connect(self):
        """
        Opens the connection to the MongoDB database using the configuration provided during instantiation.

        The connection is made using SSL encryption if the SSL parameters are provided.
        """
        self._connection_engine = MongoClient(self.create_connection_string())

    def disconnect(self):
        """
        Closes the connection to the MongoDB database.
        """
        self._connection_engine.close()

    def check_health(self):
        """
        Checks whether the connection to the MongoDB database is healthy.

        Returns:
        - A boolean value representing the health of the connection. True indicates a healthy connection.
        """
        try:
            self._connection_engine.admin.command('ismaster')
            return True
        except ServerSelectionTimeoutError:
            return False

    def create_connection_string(self):
        """
        Create the connection string for connecting to the MongoDB database.

        Returns:
        - The connection string.
        """
        connection_string = f"mongodb://{self._username}:{self._password}@{self._host}:{self._port}"

        if self._ssl:
            connection_string += "/?ssl=true"
            connection_string += f"&ssl_certfile={self._ssl_certfile_path}"
            connection_string += f"&ssl_keyfile={self._ssl_keyfile_path}"
            connection_string += f"&ssl_ca_certs={self._ssl_ca_certs}"

        return connection_string
