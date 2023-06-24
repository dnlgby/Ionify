from connections.connections_factory import ConnectionsFactory
from connections.my_sql_connection import MySQLConnection
from connections.redis_connection import RedisConnection

# Initialize a factory
factory = ConnectionsFactory()

# Register types
factory.register_type('mysql', MySQLConnection)
factory.register_type('redis', RedisConnection)