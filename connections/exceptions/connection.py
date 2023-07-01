from exceptions.ionify_exception import IonifyException


class ConnectionException(IonifyException):
    """
    Base class for connection-related exceptions.
    """

    def __init__(self, message):
        """
        Initialize the ConnectionException.

        Args:
        - message: The error message.
        """
        self.message = message


class UnknownConnectionType(ConnectionException):
    """
    Exception raised when an unknown connection type is encountered.
    """

    def __init__(self, message):
        """
        Initialize the UnknownConnectionType exception.

        Args:
        - message: The error message.
        """
        self.message = message


class MissingConfigurationKey(ConnectionException):
    """
    Exception raised when a required configuration key is missing.
    """

    def __init__(self, message):
        """
        Initialize the MissingConfigurationKey exception.

        Args:
        - message: The error message.
        """
        self.message = message
