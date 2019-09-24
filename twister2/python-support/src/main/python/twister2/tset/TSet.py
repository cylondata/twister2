import twister2.tset.TLink as tl
from twister2.tset.fn.PartitionFunc import PartitionFunc
from twister2.utils import function_wrapper


class TSet:
    def __init__(self, java_ref, env):
        self.__java_ref = java_ref
        self.__env = env

    @property
    def parallelism(self):
        return self.__java_ref.getParallelism()

    @property
    def name(self):
        return self.__java_ref.getName()

    @property
    def id(self):
        return self.__java_ref.getId()

    def direct(self):
        return tl.TLink(self.__java_ref.direct(), self.__env)

    def reduce(self, reduce_func):
        reduce_wrapper = function_wrapper(reduce_func)
        reduce_func_java_ref = self.__env.functions.reduce.build(reduce_wrapper)
        reduce_t_link_java_ref = self.__java_ref.reduce(reduce_func_java_ref)
        return tl.TLink(reduce_t_link_java_ref, self.__env)

    def all_reduce(self, reduce_func):
        reduce_wrapper = function_wrapper(reduce_func)
        reduce_func_java_ref = self.__env.functions.reduce.build(reduce_wrapper)
        reduce_t_link_java_ref = self.__java_ref.allReduce(reduce_func_java_ref)
        return tl.TLink(reduce_t_link_java_ref, self.__env)

    def gather(self):
        return tl.TLink(self.__java_ref.gather(), self.__env)

    def all_gather(self):
        return tl.TLink(self.__java_ref.allGather(), self.__env)

    def partition(self, partition_func: PartitionFunc):
        p_func_java_ref = self.__env.functions.partition.to_java_ref(partition_func)
        partition_link_java_ref = self.__java_ref.partition(p_func_java_ref)
        return tl.TLink(partition_link_java_ref, self.__env)
