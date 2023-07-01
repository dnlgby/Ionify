from redis import Redis

from connections.connection import Connection


class RedisConnection(Connection):
    """
    RedisConnection is a concrete subclass of Connection that represents a connection to a Redis database.

    Attributes:
    - _database_index: An integer representing the index of the Redis database.

    The following methods are implemented in this class:
    - from_config: A class method that creates an instance of RedisConnection from a configuration dictionary.
    - connect: Opens the connection to the Redis database.
    - disconnect: Closes the connection to the Redis database.
    - check_health: Checks whether the connection to the Redis database is healthy.
    - create_connection_string: Returns the connection string for connecting to the Redis database.
    """

    class RedisConfigKeys(Connection.ConfigKeys):
        NAME = 'name'
        HOST = 'host'
        PORT = 'port'
        DATABASE_INDEX = 'database_index'
        PASSWORD = 'password'
        SSL = 'ssl'
        SSL_KEY = 'key'
        SSL_CERT = 'cert'
        SSL_CA = 'ca'

        @classmethod
        def required_keys(cls):
            return [member.value for member in cls if
                    member.value not in [cls.SSL.value, cls.SSL_KEY.value, cls.SSL_CERT.value, cls.SSL_CA.value]]

    def __init__(self, name, host, port, database_index, password, ssl_keyfile_path=None, ssl_certfile_path=None,
                 ssl_ca_certs=None):
        super().__init__(name, host, port, None, password, ssl_keyfile_path, ssl_certfile_path, ssl_ca_certs)
        self._database_index = database_index

    @classmethod
    def from_dict(cls, config):
        """
        Create an instance of RedisConnection from a configuration dictionary.

        Args:
        - config: A dictionary containing the configuration parameters.

        Returns:
        - An instance of RedisConnection.

        Raises:
        - MissingConfigurationKey: If any required configuration keys are missing.
        """

        # Initiate config keys class
        config_keys = cls.RedisConfigKeys.required_keys()

        # Validate configuration keys
        cls.validate_dict_keys(config, config_keys)

        # Extract the keys specific to MySQL from the config
        return cls(
            config[cls.RedisConfigKeys.NAME.value],
            config[cls.RedisConfigKeys.HOST.value],
            config[cls.RedisConfigKeys.PORT.value],
            config[cls.RedisConfigKeys.DATABASE_INDEX.value],
            config[cls.RedisConfigKeys.PASSWORD.value],
            config.get(cls.RedisConfigKeys.SSL.value, {}).get(cls.RedisConfigKeys.SSL_KEY.value),
            config.get(cls.RedisConfigKeys.SSL.value, {}).get(cls.RedisConfigKeys.SSL_CERT.value),
            config.get(cls.RedisConfigKeys.SSL.value, {}).get(cls.RedisConfigKeys.SSL_CA.value)
        )

    def connect(self, **connection_addit_kwargs):
        """
        Open the connection to the Redis database.
        """
        ssl = all([self._ssl_keyfile_path, self._ssl_certfile_path, self._ssl_ca_certs])
        self._connection_engine = Redis(host=self._host,
                                        port=self._port,
                                        password=self._password,
                                        ssl=ssl,
                                        ssl_keyfile=self._ssl_keyfile_path,
                                        ssl_certfile=self._ssl_certfile_path,
                                        ssl_ca_certs=self._ssl_ca_certs,
                                        **connection_addit_kwargs)

    def disconnect(self):
        """
        Close the connection to the Redis database.
        """
        if self._connection_engine:
            self._connection_engine.close()

    def check_health(self):
        """
        Check whether the connection to the Redis database is healthy.

        Returns:
        - True if the connection is healthy, False otherwise.
        """
        try:
            self._connection_engine.ping()
            return True
        except:
            return False

    def create_connection_string(self):
        """
        Create the connection string for connecting to the Redis database.

        Returns:
        - The connection string.
        """
        return f"redis://:{self._password}@{self._host}:{self._port}/{self._database_index}"
