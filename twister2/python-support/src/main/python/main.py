from twister2.Twister2Environment import Twister2Environment
from twister2.tset.fn.SourceFunc import SourceFunc
import numpy as np

env = Twister2Environment()


class IntegerSource(SourceFunc):

    def __init__(self):
        super(IntegerSource, self).__init__()
        self.x = 0

    def has_next(self):
        return self.x < 2

    def next(self):
        self.x += 1
        return np.array([1, 1, 1])


int_source = IntegerSource()

source = env.create_source(int_source, 2)
partitioned = source.partition(env.functions.partition.load_balanced)


def map(x):
    for i in x:
        print(type(i))
    return x


mapped = partitioned.compute(map)


def sink(s):
    print(s)
    return True


direct = mapped.reduce(lambda x, y: x + y)
direct.sink(sink)
