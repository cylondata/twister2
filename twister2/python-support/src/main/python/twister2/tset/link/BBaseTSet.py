import twister2.tset.link.DirectTLink as dtl
from twister2.tset.link.BaseTSet import BaseTSet


class BBaseTSet(BaseTSet):

    def __init__(self, java_ref, env):
        super().__init__(java_ref, env)
        self.__java_ref = java_ref
        self.__env = env

    def direct(self):  # can't use return type here, due to circular dependency issue
        return dtl.DirectTLink(self.__java_ref.direct(), self.__env)
