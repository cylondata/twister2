from twister2.tset.link.BBaseTLink import BBaseTLink
from twister2.tset.sets.batch.ComputeTSet import ComputeTSet
from twister2.utils import do_args_conversion


class BIteratorTLink(BBaseTLink):

    def __init__(self, java_ref, env):
        super().__init__(java_ref, env)
        self.__java_ref = java_ref
        self.__env = env

    def map(self, lam) -> ComputeTSet:
        def map_wrapper(*args):
            print("mapping %s" % args)
            new_args = do_args_conversion(*args)
            return lam(*new_args)

        map_func_java_ref = self.__env.functions.map.build(map_wrapper)
        compute_t_set_java_ref = self.__java_ref.map(map_func_java_ref)
        return ComputeTSet(compute_t_set_java_ref, self.__env)
