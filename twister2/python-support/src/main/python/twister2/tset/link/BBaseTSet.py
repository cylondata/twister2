from twister2.tset.link import DirectTLink
from twister2.tset.link.BaseTSet import BaseTSet


class BBaseTSet(BaseTSet):

    def __init__(self, java_ref, env):
        super().__init__(java_ref, env)
        self.__java_ref = java_ref
        self.__env = env

    def direct(self) -> DirectTLink:
        return DirectTLink(self.__java_ref.direct(), self.__env)
