from twister2.Twister2Environment import Twister2Environment
from twister2.tset.fn.SourceFunc import SourceFunc

env = Twister2Environment()


class IntegerSource(SourceFunc):

    def __init__(self):
        super(IntegerSource, self).__init__()
        self.x = 0

    def has_next(self):
        return self.x < 2

    def next(self):
        self.x += 1
        return self.x


int_source = IntegerSource()

source = env.create_source(int_source, 1)
partitioned = source.partition(env.functions.partition.load_balanced)


def map(x):
    return x + 1


mapped = partitioned.map(map)


def sink(s):
    for x in s:
        print(x)
    return True


mapped.direct().sink(sink)
