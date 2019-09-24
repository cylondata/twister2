class DataPartition:

    def __init__(self, java_ref):
        self.__java_ref = java_ref

    @property
    def id(self):
        return self.__java_ref.getPartitionId()

    def consumer(self):
        from twister2.utils import IteratorWrapper
        return IteratorWrapper(self.__java_ref.getConsumer())
