import twister2.tset.TLink as Tl
from twister2.tset.fn import PartitionFunc
from twister2.utils import function_wrapper


class KeyedTSet:
    def __init__(self, java_ref, env):
        self.__java_ref = java_ref
        self.__env = env

    def direct(self):
        return Tl.TLink(self.__java_ref.keyedDirect(), self.__env)

    def keyed_reduce(self, reduce_func):
        reduce_wrapper = function_wrapper(reduce_func)
        reduce_func_java_ref = self.__env.functions.reduce.build(reduce_wrapper)
        reduce_t_link_java_ref = self.__java_ref.keyedReduce(reduce_func_java_ref)
        return Tl.TLink(reduce_t_link_java_ref, self.__env)

    def keyed_gather(self, partition_func: PartitionFunc = None,
                     key_comparator=None, use_disk=False):
        p_func_java_ref = None
        comparator_func_java_ref = None
        if partition_func is not None:
            p_func_java_ref = self.__env.functions.partition.to_java_ref(
                partition_func
            )

        if key_comparator is not None:
            key_comparator_wrapped = function_wrapper(key_comparator)
            comparator_func_java_ref = self.__env.functions.comparator.build(
                key_comparator_wrapped)

        keyed_gather_t_link_java_ref = None
        if p_func_java_ref is None and comparator_func_java_ref is None:
            keyed_gather_t_link_java_ref = self.__java_ref.keyedGather()
        elif comparator_func_java_ref is None and p_func_java_ref is not None:
            keyed_gather_t_link_java_ref = self.__java_ref.keyedGather(p_func_java_ref)
        else:
            keyed_gather_t_link_java_ref = self.__java_ref.keyedGather(
                p_func_java_ref, comparator_func_java_ref
            )

        if use_disk:
            keyed_gather_t_link_java_ref = keyed_gather_t_link_java_ref.useDisk()

        return Tl.TLink(keyed_gather_t_link_java_ref, self.__env)

    def keyed_partition(self, partition_func: PartitionFunc):
        p_func_java_ref = self.__env.functions.partition.to_java_ref(partition_func)
        partition_link_java_ref = self.__java_ref.keyedPartition(p_func_java_ref)
        return Tl.TLink(partition_link_java_ref, self.__env)

    # Functions from TSet
    def add_input(self, name, input_tset):
        self.__java_ref.addInput(name, input_tset.__java_ref)
        return self

    def cache(self):  # todo should return a cached tset instead
        return KeyedTSet(self.__java_ref.cache(), self.__env)

    def lazy_cache(self):
        return KeyedTSet(self.__java_ref.lazyCache(), self.__env)

    def persist(self):
        return KeyedTSet(self.__java_ref.persist(), self.__env)

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
