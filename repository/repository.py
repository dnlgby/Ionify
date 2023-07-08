from abc import ABC


class Repository(ABC):
    def __init__(self, datasource):
        self.datasource = datasource
