from redis import Redis

from connections.connection import Connection


class RedisConnection(Connection):
    def __init__(self, name, host, port, database_index, password, ssl_keyfile_path=None, ssl_certfile_path=None,
                 ssl_ca_certs=None):
        super().__init__(name, host, port, None, password, ssl_keyfile_path, ssl_certfile_path, ssl_ca_certs)
        self._database_index = database_index

    @classmethod
    def from_config(cls, config):
        # Extract the keys specific to Redis from the config
        return cls(
            config['name'], config['host'], config['port'], config['database_index'], config['password'],
            config.get('ssl', {}).get('key'), config.get('ssl', {}).get('cert'), config.get('ssl', {}).get('ca')
        )

    def connect(self):
        ssl = all([self._ssl_keyfile_path, self._ssl_certfile_path, self._ssl_ca_certs])
        self._connection_engine = Redis(host=self._host,
                                        port=self._port,
                                        password=self._password,
                                        ssl=ssl,
                                        ssl_keyfile=self._ssl_keyfile_path,
                                        ssl_certfile=self._ssl_certfile_path,
                                        ssl_ca_certs=self._ssl_ca_certs)

    def disconnect(self):
        if self._connection_engine:
            self._connection_engine.close()

    def check_health(self):
        try:
            self._connection_engine.ping()
            return True
        except:
            return False

    def create_connection_string(self):
        return f"redis://:{self._password}@{self._host}:{self._port}/{self._database_index}"
