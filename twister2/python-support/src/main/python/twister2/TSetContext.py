from twister2.data.DataPartition import DataPartition


class TSetContext:

    def __init__(self, java_ref):
        self.__java_ref = java_ref

    def get_input(self, key):
        return DataPartition(self.__java_ref.getInput(key))

    def get_index(self):
        return self.__java_ref.getIndex()

    @property
    def worker_id(self):
        return self.__java_ref.getWorkerId()

    @property
    def id(self):
        return self.__java_ref.getId()

    @property
    def index(self):
        return self.__java_ref.getIndex()

    @property
    def name(self):
        return self.__java_ref.getName()

    @property
    def parallelism(self):
        return self.__java_ref.getParallelism()
