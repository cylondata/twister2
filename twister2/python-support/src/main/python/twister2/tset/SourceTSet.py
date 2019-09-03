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
        p_func_j_ref = None
        
        if partition_func.pre_defined:
            p_func_j_ref = partition_func.java_ref()
        else:
            p_func_j_ref = PartitionFunctions.build(partition_func)

        return PartitionLink(self.__java_ref.partition(p_func_j_ref))
