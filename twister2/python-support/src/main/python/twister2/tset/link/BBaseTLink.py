from twister2.tset.link.TLink import TLink


class BBaseTLink(TLink):

    def __init__(self, java_ref, env):
        super().__init__(java_ref, env)
        self.__java_ref = java_ref
        self.__env = env

    def compute(self):
        pass
