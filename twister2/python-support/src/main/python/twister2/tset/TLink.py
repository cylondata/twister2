from inspect import signature

import twister2.tset.TSet as ts
from twister2.utils import function_wrapper


class TLink:

    def __init__(self, java_ref, env):
        self.__java_ref = java_ref
        self.__env = env

    def map(self, lam):
        map_wrapper = function_wrapper(lam)
        map_func_java_ref = self.__env.functions.map.build(map_wrapper)
        map_t_set_java_ref = self.__java_ref.map(map_func_java_ref)
        return ts.TSet(map_t_set_java_ref, self.__env)

    def flat_map(self, lam):
        flat_map_wrapper = function_wrapper(lam)
        flat_map_func_java_ref = self.__env.functions.flat_map.build(flat_map_wrapper)
        flat_map_t_set_java_ref = self.__java_ref.flatmap(flat_map_func_java_ref)
        return ts.TSet(flat_map_t_set_java_ref, self.__env)

    def sink(self, sink_func):
        sink_wrapper = function_wrapper(sink_func)
        sink_func_java_ref = self.__env.functions.sink.build(sink_wrapper)
        self.__java_ref.sink(sink_func_java_ref)

    def compute(self, compute_func):
        compute_wrapper = function_wrapper(compute_func)
        # if function has two arguments, user is expecting the collector version of compute
        if len(signature(compute_func).parameters) is 3:
            compute_collector_func_java_ref = self.__env.functions \
                .compute_with_collector.build(compute_wrapper)
            return ts.TSet(self.__java_ref.compute(compute_collector_func_java_ref), self.__env)
        else:
            compute_func_java_ref = self.__env.functions.compute.build(compute_wrapper)
            return ts.TSet(self.__java_ref.compute(compute_func_java_ref), self.__env)

    def for_each(self, foreach_func):
        foreach_wrapper = function_wrapper(foreach_func)
        foreach_func_java_ref = self.__env.functions.apply.build(foreach_wrapper)
        return ts.TSet(self.__java_ref.forEach(foreach_func_java_ref), self.__env)
