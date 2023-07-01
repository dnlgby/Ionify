from connections.exceptions.connection import UnknownConnectionType


class ConnectionsFactory:
    """
    ConnectionsFactory is responsible for creating connection objects based on the connection type.

    The following methods are implemented in this class:
    - register_type: Registers a connection type with its corresponding creator.
    - create: Creates a connection object based on the connection type and configuration.
    """

    def __init__(self):
        """
        Initialize the ConnectionsFactory.
        """
        self._creators = {}

    def register_type(self, connection_type, creator):
        """
        Register a connection type with its corresponding creator.

        Args:
        - connection_type: The type of the connection.
        - creator: The creator function for creating the connection object.
        """
        self._creators[connection_type] = creator

    def create(self, connection_type, config):
        """
        Create a connection object based on the connection type and configuration.

        Args:
        - connection_type: The type of the connection.
        - config: The configuration for the connection.

        Returns:
        - A connection object created based on the configuration.

        Raises:
        - UnknownConnectionType: If the connection type is not registered.
        """
        creator = self._creators.get(connection_type)
        if not creator:
            raise UnknownConnectionType(f"Connection type {connection_type} is unknown.")

        return creator.from_dict(config)
