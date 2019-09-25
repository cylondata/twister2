from twister2.data.DataPartition import DataPartition


class DataObject:

    def __init__(self, java_ref):
        self.__java_ref = java_ref

    @property
    def partition_count(self):
        return self.__java_ref.getPartitionCount()

    @property
    def id(self):
        return self.__java_ref.getID()

    def get_partition(self, partition_id):
        return DataPartition(self.__java_ref.getPartition(partition_id))
