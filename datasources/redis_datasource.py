import json

from connections import RedisConnection
from datasources.datasource import DataSource


class RedisDataSource(DataSource):
    """
    RedisDataSource is a concrete subclass of DataSource that interfaces with a Redis database.

    This class is designed to work with redis-py, which is a Python interface to the Redis key-value store.

    Methods:
    - connect: Establishes a connection to the Redis server.
    - disconnect: Disconnects from the Redis server.
    - set_key: Sets the value of a key in Redis.
    - get_key: Retrieves the value of a key from Redis.
    - delete_key: Deletes a key from Redis.
    - key_exists: Checks if a key exists in Redis.
    - set_hash_field: Sets the value of a field in a Redis hash.
    - get_hash_field: Retrieves the value of a field from a Redis hash.
    - delete_hash_field: Deletes a field from a Redis hash.
    - set_set_value: Adds a value to a Redis set.
    - get_set_values: Retrieves all values from a Redis set.
    - remove_set_value: Removes a value from a Redis set.
    - set_json_value: Sets the value of a key in Redis as a JSON object using RedisJSON.
    - get_json_value: Retrieves the value of a key from Redis as a JSON object using RedisJSON.

    Note: This implementation assumes the availability of appropriate Redis commands in the underlying connection engine.
    """

    def __init__(self, connection: RedisConnection):
        super().__init__(connection)

    def set_key(self, key: str, value: str):
        """
        Set the value of a key in Redis.

        Args:
        - key: The key to set.
        - value: The value to set.

        Returns:
        - True if the operation was successful, False otherwise.
        """
        return self._connection_engine.set(key, value)

    def get_key(self, key: str):
        """
        Retrieve the value of a key from Redis.

        Args:
        - key: The key to retrieve.

        Returns:
        - The value of the key if it exists, None otherwise.
        """
        return self._connection_engine.get(key)

    def delete_key(self, key: str):
        """
        Delete a key from Redis.

        Args:
        - key: The key to delete.

        Returns:
        - The number of keys deleted.
        """
        return self._connection_engine.delete(key)

    def key_exists(self, key: str):
        """
        Check if a key exists in Redis.

        Args:
        - key: The key to check.

        Returns:
        - True if the key exists, False otherwise.
        """
        return self._connection_engine.exists(key)

    def set_hash_field(self, key: str, field: str, value: str):
        """
        Set the value of a field in a Redis hash.

        Args:
        - key: The key of the hash.
        - field: The field to set.
        - value: The value to set.

        Returns:
        - True if the operation was successful, False otherwise.
        """
        return self._connection_engine.hset(key, field, value)

    def get_hash_field(self, key: str, field: str):
        """
        Retrieve the value of a field from a Redis hash.

        Args:
        - key: The key of the hash.
        - field: The field to retrieve.

        Returns:
        - The value of the field if it exists, None otherwise.
        """
        return self._connection_engine.hget(key, field)

    def delete_hash_field(self, key: str, field: str):
        """
        Delete a field from a Redis hash.

        Args:
        - key: The key of the hash.
        - field: The field to delete.

        Returns:
        - The number of fields deleted.
        """
        return self._connection_engine.hdel(key, field)

    def set_set_value(self, key: str, value: str):
        """
        Add a value to a Redis set.

        Args:
        - key: The key of the set.
        - value: The value to add.

        Returns:
        - The number of elements added to the set.
        """
        return self._connection_engine.sadd(key, value)

    def get_set_values(self, key: str):
        """
        Retrieve all values from a Redis set.

        Args:
        - key: The key of the set.

        Returns:
        - A set containing all values in the set.
        """
        return self._connection_engine.smembers(key)

    def remove_set_value(self, key: str, value: str):
        """
        Remove a value from a Redis set.

        Args:
        - key: The key of the set.
        - value: The value to remove.

        Returns:
        - The number of elements removed from the set.
        """
        return self._connection_engine.srem(key, value)

    def set_json_value(self, key: str, value):
        """
        Sets the value of a key in Redis as a JSON object using RedisJSON.

        Args:
        - key: The key to set.
        - value: The JSON object to set.

        Returns:
        - True if the operation was successful, False otherwise.
        """
        json_value = json.dumps(value)
        return self._connection_engine.execute_command('JSON.SET', key, '.', json_value)

    def get_json_value(self, key: str):
        """
        Retrieves the value of a key from Redis as a JSON object using RedisJSON.

        Args:
        - key: The key to retrieve.

        Returns:
        - The JSON object value if the key exists and is a valid JSON, None otherwise.
        """
        json_response = self._connection_engine.execute_command('JSON.GET', key)
        if json_response is not None:
            try:
                return json.loads(json_response)
            except json.JSONDecodeError:
                pass
        return None
