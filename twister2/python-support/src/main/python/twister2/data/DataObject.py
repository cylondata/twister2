from twister2.data.DataPartition import DataPartition


class DataObject:

    def __init__(self, java_ref, env):
        self.__java_ref = java_ref
        self.__env = env

    @property
    def partition_count(self):
        return self.__java_ref.getPartitionCount()

    @property
    def id(self):
        return self.__java_ref.getID()

    def get_partitions(self):
        java_partitions = self.__java_ref.getPartitions()
        python_partitions = []
        for jp in java_partitions:
          python_partitions.append(DataPartition(jp, self.__env))
        return python_partitions

    def get_partition(self, partition_id):
        return DataPartition(self.__java_ref.getPartition(partition_id), self.__env)
