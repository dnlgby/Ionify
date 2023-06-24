import yaml


class ConnectionsConfigurationParser:
    """
    ConnectionsConfigurationParser is responsible for parsing the connections configuration from a YAML file.

    Attributes:
    - _connections_factory: An instance of the connection factory.

    The following methods are implemented in this class:
    - parse_connections_config: Parses the connections configuration from a YAML file and creates connection objects.
    """

    def __init__(self, connection_factory):
        """
        Initialize the ConnectionsConfigurationParser.

        Args:
        - connection_factory: An instance of the connection factory.
        """
        self._connections_factory = connection_factory

    def parse_connections_config(self, connections_yaml_file_path):
        """
        Parse the connections configuration from a YAML file and create connection objects.

        Args:
        - connections_yaml_file_path: The path to the connections YAML file.

        Returns:
        - A list of connection objects created from the configuration.

        Raises:
        - FileNotFoundError: If the connections YAML file is not found.
        """
        with open(connections_yaml_file_path, 'r') as file:
            connections_config = yaml.safe_load(file)

        connections = []
        for connection_config in connections_config['connections']:
            connection_type = connection_config['type']
            connection = self._connections_factory.create(connection_type, connection_config)
            connections.append(connection)

        return connections
