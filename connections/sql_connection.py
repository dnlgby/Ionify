from abc import ABC, abstractmethod

from sqlalchemy import exc, text
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

from connections.connection import Connection


class SQLConnection(Connection, ABC):
    """
    SQLConnection is an abstract base class that represents a generic connection to a SQL database.

    Attributes:
    - _database: A string representing the name of the database.
    - _session_maker: An SQLAlchemy sessionmaker instance for managing sessions with the database.
    - _automap_base_model: An SQLAlchemy AutomapBase instance for automatically generating ORM classes from database tables.
    - _declarative_base_model: An SQLAlchemy declarative base class for declaring new models.
    - _user_defined_models: A list of user-defined SQLAlchemy model classes.

    The following methods are implemented in this class:
    - connect: Opens the connection to the database.
    - disconnect: Closes the connection to the database.
    - check_health: Checks whether the connection to the database is healthy.
    - register_model: Registers a user-defined model.
    - create_all_user_defined_models: Creates tables for all user-defined models.
    - declarative_base_model: Property that returns the declarative_base_model.

    The following methods are required to be implemented in any child class:
    - create_connection_string: Returns the connection string specific to the type of SQL database.
    """

    def __init__(self, name, host, port, database, username, password, ssl_keyfile_path=None, ssl_certfile_path=None,
                 ssl_ca_certs=None):
        super().__init__(name, host, port, username, password, ssl_keyfile_path, ssl_certfile_path, ssl_ca_certs)
        self._database = database
        self._session_maker = None
        self._automap_base_model = None
        self._declarative_base_model = None
        self._user_defined_models = []
        self._initiate_declarative_base_model()

    @property
    def declarative_base_model(self):
        """Get the SQLAlchemy declarative base model class instance."""
        return self._declarative_base_model

    @property
    def automap_base_model(self):
        """Get the SQLAlchemy declarative automap base model class instance."""
        return self._automap_base_model

    @abstractmethod
    def _create_engine(self):
        pass

    def get_new_session(self):
        """Get new SQLAlchemy session"""
        return self._session_maker()

    def connect(self):
        """
        Open the connection to the SQL database.
        """
        self._create_engine()
        self._session_maker = sessionmaker(bind=self._connection_engine)

        # Auto map base - Initiate models for the existing tables in the database.
        self._initiate_automap_base_model()

    def _initiate_automap_base_model(self):
        """
        Prepare AutomapBase model.
        """
        self._automap_base_model = automap_base()
        self._automap_base_model.prepare(self._connection_engine, reflect=True)

    def _initiate_declarative_base_model(self):
        """
        Initiate declarative base model for creating new models.
        """
        self._declarative_base_model = declarative_base()

    def register_model(self, model):
        """
        Register a user-defined model.

        Args:
        - model: A SQLAlchemy model class.
        """
        assert issubclass(model, self._declarative_base_model), "Model must be a subclass of Base."
        self._user_defined_models.append(model)

    def create_all_user_defined_models(self):
        """
        Create tables for all user-defined models that have been registered.
        """
        self._declarative_base_model.metadata.create_all(self._connection_engine)

    def disconnect(self):
        """
        Close the connection to the SQL database.
        SQLAlchemy engine doesn't require explicit disconnect. It's handled automatically.
        """
        pass

    def check_health(self):
        """
        Check whether the connection to the SQL database is healthy.

        Returns:
        - True if the connection is healthy, False otherwise.
        """
        try:
            session = self.get_new_session()  # Using the get_new_session method in SQLConnection
            session.execute(text("SELECT 1"))
            session.close()
            return True
        except exc.DBAPIError:
            return False

    @abstractmethod
    def create_connection_string(self):
        """
        Create a connection string specific to the type of SQL database.

        Returns:
        - The connection string.
        """
        pass
