from twister2 import TSetContext
from twister2.Twister2Environment import Twister2Environment
from twister2.tset.fn.SourceFunc import SourceFunc
import numpy as np

env = Twister2Environment(resources=[{"cpu": 1, "ram": 512, "instances": 4}])

print("Hello from worker %d" % env.worker_id)


class IntSource(SourceFunc):

    def __init__(self):
        super().__init__()
        self.i = 0

    def has_next(self):
        return self.i < 10

    def next(self):
        res = self.i
        self.i = self.i + 1
        return np.array([res, res + 1, res + 2])


def read_test_tuples(itr, collector, ctx: TSetContext):
    for item in itr:
        print(type(item), item)
        collector.collect(item.tolist())


source_x = env.create_source(IntSource(), 4)

data = source_x.cache()

d = data.get_data()
for p in d.get_partitions():
    for v in p.consumer():
        print("v ", v)
