from twister2.tset.link.BIteratorTLink import BIteratorTLink


class DirectTLink(BIteratorTLink):

    def __init__(self, java_ref, env):
        super().__init__(java_ref, env)
