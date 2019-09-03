from twister2.tset.sets.batch.ComputeTSet import ComputeTSet


class PartitionLink:

    def __init__(self, java_ref):
        self.__java_ref = java_ref

    def map(self) -> ComputeTSet:
        return self.__java_ref.map()
