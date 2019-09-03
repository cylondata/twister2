from twister2.Twister2Environment import Twister2Environment
from twister2.tset.fn.SourceFunc import SourceFunc

ctx = Twister2Environment()


class IntegerSource(SourceFunc):

    def __init__(self):
        super(IntegerSource, self).__init__()
        self.x = 0

    def has_next(self):
        return self.x < 100

    def next(self):
        self.x += 1
        return self.x


int_source = IntegerSource()

source = ctx.create_source(int_source, 4)

source.partition(ctx.functions.partition.load_balanced)
