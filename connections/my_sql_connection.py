from sqlalchemy import create_engine

from connections.connection import Connection


class MySQLConnection(Connection):
    def __init__(self, name, host, port, database, username, password, ssl_keyfile_path=None, ssl_certfile_path=None,
                 ssl_ca_certs=None):
        super().__init__(name, host, port, username, password, ssl_keyfile_path, ssl_certfile_path, ssl_ca_certs)
        self._database = database

    @classmethod
    def from_config(cls, config):

        # Validate configuration keys
        cls.validate_config_keys(config, ['name', 'host', 'port', 'database', 'username', 'password'])

        # Extract the keys specific to MySQL from the config
        return cls(
            config['name'], config['host'], config['port'], config['database'], config['username'], config['password'],
            config.get('ssl', {}).get('key'), config.get('ssl', {}).get('cert'), config.get('ssl', {}).get('ca')
        )

    def connect(self):
        connection_string = self.create_connection_string()
        self._connection_engine = create_engine(connection_string)

    def disconnect(self):
        # SQLAlchemy engine doesn't require explicit disconnect. It's handled automatically.
        pass

    def check_health(self):
        try:
            self._connection_engine.execute("SELECT 1")
            return True
        except:
            return False

    def create_connection_string(self):
        return f"mysql+pymysql://{self._username}:{self._password}@{self._host}:{self._port}/{self._database}"
