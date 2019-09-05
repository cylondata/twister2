from twister2.tset.link.BBaseTSet import BBaseTSet


class ComputeTSet(BBaseTSet):

    def __init__(self, java_ref, env):
        BBaseTSet.__init__(self, java_ref, env)
        self.__java_ref = java_ref
        self.__env = env
