from abc import ABC, abstractmethod


class PartitionFunc(ABC):
    def __init__(self):
        pass

    @abstractmethod
    def prepare(self, sources: set, destinations: set):
        pass

    @abstractmethod
    def partition(self, source_index: int, val) -> int:
        pass

    @abstractmethod
    def commit(self, source_index: int, partition: int) -> int:
        pass

    @property
    def pre_defined(self) -> bool:
        return False

    def java_ref(self):
        return None
