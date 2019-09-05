import cloudpickle as cp

from twister2.tset.fn.PartitionFunc import PartitionFunc


class PartitionFunctions:

    def __init__(self, java_ref, env):
        self.__java_ref = java_ref
        self.__env = env

    @property
    def load_balanced(self):
        return JavaWrapper(self.__java_ref.loadBalanced())

    def build(self, partition_func: PartitionFunc):
        # send python dump to java -> create a java object in JVM -> get the ref back
        return self.__java_ref.build(cp.dumps(partition_func))

    def to_java_ref(self, partition_func: PartitionFunc):
        if partition_func.pre_defined:
            return partition_func.java_ref()
        else:
            return self.build(partition_func)


class JavaWrapper(PartitionFunc):

    def prepare(self, sources: set, destinations: set):
        self.__java_ref.prepare(sources, destinations)

    def partition(self, source_index: int, val) -> int:
        return self.__java_ref.partition(source_index, val)

    def commit(self, source_index: int, partition: int) -> int:
        return self.__java_ref.commit(source_index, partition)

    def pre_defined(self) -> bool:
        return True

    def java_ref(self):
        return self.__java_ref

    def __init__(self, java_ref):
        super().__init__()
        self.__java_ref = java_ref
