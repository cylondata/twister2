from abc import ABC, abstractmethod


class SourceFunc(ABC):

    def __init__(self):
        pass

    @abstractmethod
    def has_next(self):
        return False

    @abstractmethod
    def next(self):
        return None
