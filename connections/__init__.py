from connections.connections_factory import ConnectionsFactory
from connections.connections_parser import ConnectionsConfigurationParser
from connections.my_sql_connection import MySQLConnection
from connections.redis_connection import RedisConnection

# Initialize a factory
factory = ConnectionsFactory()

# Register types
factory.register_type('mysql', MySQLConnection)
factory.register_type('redis', RedisConnection)

# Initialize an yaml configuration parser
parser = ConnectionsConfigurationParser(factory)
