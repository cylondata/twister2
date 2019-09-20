import numpy as np

from twister2.Twister2Environment import Twister2Environment
from twister2.tset.fn.SourceFunc import SourceFunc

env = Twister2Environment(name="MyPython", config={"YOYO": 123}, resources=[{"cpu": 1, "ram": 1024, "instances": 2}])


class IntegerSource(SourceFunc):

    def __init__(self):
        super(IntegerSource, self).__init__()
        self.x = 0

    def has_next(self):
        return self.x < 2

    def next(self):
        self.x += 1
        return np.array([[1, 1, 1], [1, 1, 1.1]])


int_source = IntegerSource()

source = env.create_source(int_source, 4)
partitioned = source.partition(env.functions.partition.load_balanced)


def map(x):
    x[1, 1] = 200
    return x.reshape(6, 1)


mapped = partitioned.map(map)


def sink(s):
    for i in s:
        print(i)
    return True


direct = mapped.direct()
direct.sink(sink)
