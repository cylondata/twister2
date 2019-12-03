from twister2.tset.fn.factory.GenericFunctions import GenericFunctions
from twister2.tset.fn.factory.PartitionFunctions import PartitionFunctions


class TSetFunctions:

    def __init__(self, java_ref, env):
        self.__java_ref = java_ref
        self.__partition_functions = PartitionFunctions(java_ref.partition(), env)
        self.__map_functions = GenericFunctions(java_ref.map(), env)
        self.__compute_functions = GenericFunctions(java_ref.compute(), env)
        self.__compute_collector_functions = GenericFunctions(java_ref.computeCollector(), env)
        self.__sink_functions = GenericFunctions(java_ref.sink(), env)
        self.__reduce_functions = GenericFunctions(java_ref.reduce(), env)
        self.__apply_functions = GenericFunctions(java_ref.apply(), env)
        self.__flat_map_functions = GenericFunctions(java_ref.flatMap(), env)
        self.__comparator_functions = GenericFunctions(java_ref.comparator(), env)

    @property
    def partition(self) -> PartitionFunctions:
        return self.__partition_functions

    @property
    def map(self) -> GenericFunctions:
        return self.__map_functions

    @property
    def flat_map(self) -> GenericFunctions:
        return self.__flat_map_functions

    @property
    def compute(self) -> GenericFunctions:
        return self.__compute_functions

    @property
    def compute_with_collector(self) -> GenericFunctions:
        return self.__compute_collector_functions

    @property
    def sink(self) -> GenericFunctions:
        return self.__sink_functions

    @property
    def apply(self) -> GenericFunctions:
        return self.__apply_functions

    @property
    def reduce(self) -> GenericFunctions:
        return self.__reduce_functions

    @property
    def comparator(self) -> GenericFunctions:
        return self.__comparator_functions
