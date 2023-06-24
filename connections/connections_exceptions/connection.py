from exceptions.ionify_exception import IonifyException

class ConnectionException(IonifyException):
    def __init__(self, message):
        self.message = message


class UnknownConnectionType(ConnectionException):
    def __init__(self, message):
        self.message = message

class MissingConfigurationKey(ConnectionException):
    def __init__(self, message):
        self.message = message