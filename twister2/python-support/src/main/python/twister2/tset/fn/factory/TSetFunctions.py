from twister2.tset.fn.factory.MapFunctions import MapFunctions
from twister2.tset.fn.factory.PartitionFunctions import PartitionFunctions


class TSetFunctions:

    def __init__(self, java_ref, env):
        self.__java_ref = java_ref
        self.__partition_functions = PartitionFunctions(java_ref.partition(), env)
        self.__map_functions = MapFunctions(java_ref.map(), env)

    @property
    def partition(self) -> PartitionFunctions:
        return self.__partition_functions

    @property
    def map(self):
        return self.__map_functions
