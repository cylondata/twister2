from twister2.tset.fn.PartitionFunc import PartitionFunc
from twister2.tset.fn.factory.PartitionFunctions import PartitionFunctions
from twister2.tset.link import DirectLink
from twister2.tset.link.PartitionLink import PartitionLink


class SourceTSet:

    def __init__(self, java_ref):
        self.__java_ref = java_ref

    def direct(self) -> DirectLink:
        return self.__java_ref.direct()

    def partition(self, partition_func: PartitionFunc) -> PartitionLink:
        p_func_java_ref = PartitionFunctions.to_java_ref(partition_func)
        partition_link_java_ref = self.__java_ref.partition(p_func_java_ref)
        return PartitionLink(partition_link_java_ref)
