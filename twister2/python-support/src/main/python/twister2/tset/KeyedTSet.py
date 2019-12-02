import twister2.tset.TLink as Tl
from twister2.utils import function_wrapper


class KeyedTSet:
    def __init__(self, java_ref, env):
        self.__java_ref = java_ref
        self.__env = env

    def direct(self):
        return Tl.TLink(self.__java_ref.keyedDirect(), self.__env)

    def reduce(self, reduce_func):
        reduce_wrapper = function_wrapper(reduce_func)
        reduce_func_java_ref = self.__env.functions.reduce.build(reduce_wrapper)
        reduce_t_link_java_ref = self.__java_ref.keyedReduce(reduce_func_java_ref)
        return Tl.TLink(reduce_t_link_java_ref, self.__env)

    # TLink functions

    def for_each(self, foreach_func):
        return self.direct().for_each(foreach_func)
