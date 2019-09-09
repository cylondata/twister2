from twister2.tset.fn.factory.ComputeFunctions import ComputeFunctions
from twister2.tset.fn.factory.MapFunctions import MapFunctions
from twister2.tset.fn.factory.PartitionFunctions import PartitionFunctions
from twister2.tset.fn.factory.SinkFunctions import SinkFunctions


class TSetFunctions:

    def __init__(self, java_ref, env):
        self.__java_ref = java_ref
        self.__partition_functions = PartitionFunctions(java_ref.partition(), env)
        self.__map_functions = MapFunctions(java_ref.map(), env)
        self.__compute_functions = ComputeFunctions(java_ref.compute(), env)
        self.__sink_functions = SinkFunctions(java_ref.sink(), env)

    @property
    def partition(self) -> PartitionFunctions:
        return self.__partition_functions

    @property
    def map(self):
        return self.__map_functions

    @property
    def compute(self):
        return self.__compute_functions

    @property
    def sink(self):
        return self.__sink_functions
