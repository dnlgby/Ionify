
from connections import parser

if __name__ == "__main__":
    connections = parser.parse_connections_config('connections.yaml')
    print(connections)