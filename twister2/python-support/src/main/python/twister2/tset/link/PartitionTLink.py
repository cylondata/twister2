import twister2.Twister2Environment as TEnv
from twister2.tset.sets.batch.ComputeTSet import ComputeTSet


class PartitionTLink:

    def __init__(self, java_ref, env: TEnv):
        self.__java_ref = java_ref
        self.__env = env

    def map(self, lam) -> ComputeTSet:
        map_func_java_ref = self.__env.functions.map.build(lam)
        compute_t_set_java_ref = self.__java_ref.map(map_func_java_ref)
        return ComputeTSet(compute_t_set_java_ref, self.__env)
