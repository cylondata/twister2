import twister2.tset.TLink as tl
from twister2.tset.fn.PartitionFunc import PartitionFunc
from twister2.utils import function_wrapper
from twister2.data.DataObject import DataObject


class TSet:
    def __init__(self, java_ref, env, cached=False):
        self.__java_ref = java_ref
        self.__env = env
        self.__cached = cached

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

    def add_input(self, name, input_tset):
        self.__java_ref.addInput(name, input_tset.__java_ref)
        return self

    def cache(self):  # todo should return a cached tset instead
        return TSet(self.__java_ref.cache(), self.__env, cached=True)

    def lazy_cache(self):
        return TSet(self.__java_ref.lazyCache(), self.__env)

    def persist(self):
        return TSet(self.__java_ref.persist(), self.__env)

    def get_data(self):
        if self.__cached:
          return DataObject(self.__java_ref.getDataObject(), self.__env)
        return None

    # TLink functions

    def map(self, lam):
        return self.direct().map(lam)

    def flat_map(self, lam):
        return self.direct().flat_map(lam)

    def sink(self, sink_func):
        return self.direct().sink(sink_func)

    def compute(self, compute_func):
        return self.direct().compute(compute_func)

    def for_each(self, foreach_func):
        return self.direct().for_each(foreach_func)
