from twister2.tset.fn.PartitionFunc import PartitionFunc
from twister2.tset.link import DirectTLink
from twister2.tset.link.PartitionTLink import PartitionTLink


class SourceTSet:

    def __init__(self, java_ref, env):
        self.__java_ref = java_ref
        self.__env = env

    def direct(self) -> DirectTLink:
        return self.__java_ref.direct()

    def partition(self, partition_func: PartitionFunc) -> PartitionTLink:
        p_func_java_ref = self.__env.functions.partition.to_java_ref(partition_func)
        partition_link_java_ref = self.__java_ref.partition(p_func_java_ref)
        return PartitionTLink(partition_link_java_ref, self.__env)
