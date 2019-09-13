import twister2.Twister2Environment as TEnv
from twister2.tset.link.BIteratorTLink import BIteratorTLink


class PartitionTLink(BIteratorTLink):

    def __init__(self, java_ref, env: TEnv):
        super().__init__(java_ref, env)
        self.__java_ref = java_ref
        self.__env = env
