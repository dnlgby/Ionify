from sqlalchemy.orm import aliased
from sqlalchemy.sql import func

from datasources.sql_datasource import SQLDataSource


class PostgreSQLDataSource(SQLDataSource):
    """
    PostgreSQLDataSource is a concrete subclass of SQLDataSource that interfaces with a PostgreSQL database.

    This class is designed to work with SQLAlchemy ORM, which allows high-level and Pythonic manipulation of SQL databases.

    Methods:
    - insert: Inserts a new record into a table in the PostgreSQL database.
    - update: Updates an existing record in a table in the PostgreSQL database.
    - remove: Deletes an existing record from a table in the PostgreSQL database.
    - query: Executes a SQL query against the PostgreSQL database.
    """

    def __init__(self, connection):
        super().__init__(connection)

    def insert(self, data_entity_key: str, data: dict):
        session = self.get_new_session()
        try:
            instance = self.get_model(data_entity_key)(**data)
            session.add(instance)
            session.commit()
            return instance.id
        finally:
            session.close()

    def update(self, data_entity_key: str, data_entity_id, data: dict):
        session = self.get_new_session()
        try:
            instance = session.query(self.get_model(data_entity_key)).get(data_entity_id)
            for key, value in data.items():
                setattr(instance, key, value)
            session.commit()
            return True
        finally:
            session.close()

    def remove(self, data_entity_key: str, data_entity_id):
        session = self.get_new_session()
        try:
            instance = session.query(self.get_model(data_entity_key)).get(data_entity_id)
            session.delete(instance)
            session.commit()
            return True
        finally:
            session.close()

    def query(self, query_string: str):
        result = self._connection.connection_engine.execute(query_string)
        return result.fetchall()

    def find_by_id(self, data_entity_key: str, data_entity_id):
        session = self.get_new_session()
        try:
            instance = session.query(self.get_model(data_entity_key)).get(data_entity_id)
            return instance
        finally:
            session.close()

    def find_all(self, data_entity_key: str):
        session = self.get_new_session()
        try:
            all_instances = session.query(self.get_model(data_entity_key)).all()
            return all_instances
        finally:
            session.close()

    def find_by_field(self, data_entity_key: str, field_name: str, field_value):
        session = self.get_new_session()
        try:
            instances = session.query(self.get_model(data_entity_key)).filter(
                getattr(self.get_model(data_entity_key), field_name) == field_value).all()
            return instances
        finally:
            session.close()

    def count(self, data_entity_key: str):
        session = self.get_new_session()
        try:
            count = session.query(func.count(self.get_model(data_entity_key).id)).scalar()
            return count
        finally:
            session.close()

    def exists(self, data_entity_key: str, data_entity_id):
        session = self.get_new_session()
        try:
            instance = session.query(self.get_model(data_entity_key)).get(data_entity_id)
            return instance is not None
        finally:
            session.close()

    def inner_join(self, primary_entity_key: str, secondary_entity_key: str, on_field: str):
        session = self.get_new_session()
        try:
            primary = self.get_model(primary_entity_key)
            secondary = aliased(self.get_model(secondary_entity_key))
            join_result = session.query(primary).join(secondary,
                                                      getattr(primary, on_field) == getattr(secondary, on_field)).all()
            return join_result
        finally:
            session.close()

    def left_join(self, primary_entity_key: str, secondary_entity_key: str, on_field: str):
        session = self.get_new_session()
        try:
            primary = self.get_model(primary_entity_key)
            secondary = aliased(self.get_model(secondary_entity_key))
            join_result = session.query(primary).outerjoin(secondary,
                                                           getattr(primary, on_field) == getattr(secondary,
                                                                                                 on_field)).all()

            return join_result
        finally:
            session.close()

    def right_join(self, primary_entity_key: str, secondary_entity_key: str, on_field: str):
        session = self.get_new_session()
        try:
            primary = aliased(self.get_model(primary_entity_key))
            secondary = self.get_model(secondary_entity_key)
            join_result = session.query(secondary).outerjoin(primary,
                                                             getattr(primary, on_field) == getattr(secondary,
                                                                                                   on_field)).all()
            return join_result
        finally:
            session.close()
