from sqlalchemy import text
from sqlalchemy.orm import aliased

from datasources.sql_datasource import SQLDataSource


class MySQLDataSource(SQLDataSource):
    """
    MySQLDataSource is a concrete subclass of SQLDataSource that interfaces with a MySQL database.

    This class is designed to work with SQLAlchemy ORM, which allows high-level and Pythonic manipulation of SQL databases.

    Methods:
    - insert: Inserts a new record into a table in the MySQL database.
    - update: Updates an existing record in a table in the MySQL database.
    - remove: Deletes an existing record from a table in the MySQL database.
    - query: Executes a SQL query against the MySQL database.
    - find_by_id: Fetches a record by its id from a table in the MySQL database.
    - find_all: Fetches all records from a table in the MySQL database. An optional condition can be applied.
    - count: Counts all records in a table in the MySQL database. An optional condition can be applied.
    - exists: Checks if a record exists in a table in the MySQL database.
    - inner_join: Performs an inner join operation between two tables in the MySQL database. An optional condition can be applied.
    - left_join: Performs a left outer join operation between two tables in the MySQL database. An optional condition can be applied.
    - right_join: Performs a right outer join operation between two tables in the MySQL database. An optional condition can be applied.
    """

    def __init__(self, connection):
        super().__init__(connection)

    def _apply_condition(self, query, condition):
        if condition:
            query = query.filter(text(condition))
        return query

    def insert(self, data_entity_key: str, data: dict):
        session = self.get_new_session()
        try:
            instance = self.get_model(data_entity_key)(**data)
            session.add(instance)
            session.commit()
            return instance.id
        finally:
            session.close()

    def update(self, data_entity_key: str, data_entity_id: int, data: dict):
        session = self.get_new_session()
        try:
            query = session.query(self.get_model(data_entity_key))
            instance = query.get(data_entity_id)
            for key, value in data.items():
                setattr(instance, key, value)
            session.commit()
            return True
        finally:
            session.close()

    def remove(self, data_entity_key: str, data_entity_id: int):
        session = self.get_new_session()
        try:
            instance = session.query(self.get_model(data_entity_key)).get(data_entity_id)
            session.delete(instance)
            session.commit()
            return True
        finally:
            session.close()

    def query(self, query_string: str):
        session = self.get_new_session()
        result = session.execute(text(query_string))
        session.close()
        return result

    def find_by_id(self, data_entity_key: str, data_entity_id: int):
        session = self.get_new_session()
        try:
            instance = session.query(self.get_model(data_entity_key)).get(data_entity_id)
            return instance
        finally:
            session.close()

    def find_all(self, data_entity_key: str, condition=None):
        session = self.get_new_session()
        try:
            query = session.query(self.get_model(data_entity_key))
            query = self._apply_condition(query, condition)
            all_instances = query.all()
            return all_instances
        finally:
            session.close()

    def count(self, data_entity_key: str, condition=None):
        session = self.get_new_session()
        try:
            query = session.query(self.get_model(data_entity_key))
            query = self._apply_condition(query, condition)
            count = query.count()
            return count
        finally:
            session.close()

    def exists(self, data_entity_key: str, data_entity_id: int):
        session = self.get_new_session()
        try:
            instance = session.query(self.get_model(data_entity_key)).get(data_entity_id)
            return instance is not None
        finally:
            session.close()

    def inner_join(self, primary_entity_key: str, secondary_entity_key: str, on_field: str, condition=None):
        session = self.get_new_session()
        try:
            primary = self.get_model(primary_entity_key)
            secondary = aliased(self.get_model(secondary_entity_key))
            query = session.query(primary, secondary).join(
                secondary, getattr(primary, on_field) == getattr(secondary, on_field))
            query = self._apply_condition(query, condition)
            join_result = query.all()
            return join_result
        finally:
            session.close()

    def left_join(self, primary_entity_key: str, secondary_entity_key: str, on_field: str, condition=None):
        session = self.get_new_session()
        try:
            primary = self.get_model(primary_entity_key)
            secondary = aliased(self.get_model(secondary_entity_key))
            query = session.query(primary, secondary).outerjoin(
                secondary, getattr(primary, on_field) == getattr(secondary, on_field))
            query = self._apply_condition(query, condition)
            join_result = query.all()
            return join_result
        finally:
            session.close()

    def right_join(self, primary_entity_key: str, secondary_entity_key: str, on_field: str, condition=None):
        session = self.get_new_session()
        try:
            primary = aliased(self.get_model(primary_entity_key))
            secondary = self.get_model(secondary_entity_key)
            query = session.query(secondary, primary).outerjoin(
                primary, getattr(primary, on_field) == getattr(secondary, on_field))
            query = self._apply_condition(query, condition)
            join_result = query.all()
            return join_result
        finally:
            session.close()
