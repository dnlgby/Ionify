from connections_exceptions.connection import UnknownConnectionType


class ConnectionsFactory:

    def __init__(self):
        self._creators = {}

    def register_type(self, connection_type, creator):
        self._creators[connection_type] = creator

    def create(self, connection_type, config):
        creator = self._creators.get(connection_type)
        if not creator:
            raise UnknownConnectionType(f"connection type {connection_type} is unknown.")
        
        return creator.from_config(config)

