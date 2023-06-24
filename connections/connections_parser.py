import yaml


class ConnectionsConfigParser:

    def __init__(self, connection_factory):
        self._connections_factory = connection_factory

    def parse_connections_config(self, connections_yaml_file_path):
        with open(connections_yaml_file_path, 'r') as file:
            connections_config = yaml.safe_load(file)

        connections = []
        for connection_config in connections_config['connections']:
            connection_type = connection_config['type']
            connection = self._connections_factory.create(connection_type, connection_config)
            connections.append(connection)

        return connections


if __name__ == "__main__":
    from connections import factory
    a = ConnectionsConfigParser(factory)
    print(a.parse_connections_config(connections_yaml_file_path='connections.yaml'))