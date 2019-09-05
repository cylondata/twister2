from twister2.tset.link.BBaseTSet import BBaseTSet


class ComputeTSet(BBaseTSet):

    def __init__(self, java_ref, env):
        BBaseTSet.__init__(self, java_ref.super, env)
        self.__java_ref = java_ref
