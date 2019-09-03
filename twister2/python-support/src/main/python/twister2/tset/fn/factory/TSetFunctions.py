from twister2.tset.fn.factory.PartitionFunctions import PartitionFunctions


class TSetFunctions:

    def __init__(self, java_ref):
        self.java_ref = java_ref
        self.partition_functions = PartitionFunctions(java_ref.partition())

    @property
    def partition(self) -> PartitionFunctions:
        return self.partition_functions
