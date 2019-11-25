from twister2 import TSetContext
from twister2.Twister2Environment import Twister2Environment
from twister2.tset.fn.SourceFunc import SourceFunc
from datetime import datetime

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
        return res


source_x = env.create_source(IntSource(), 4)


def mul_by_five(itr, collector, ctx: TSetContext):
    for i in itr:
        collector.collect(i * 5)


def add_two(itr, collector, ctx: TSetContext):
    for i in itr:
        collector.collect(i + 2)


two_computes = source_x.compute(mul_by_five).compute(add_two)

cached = two_computes.cache()

source_z = env.create_source(IntSource(), 4)


def combine_x_and_z(itr, collector, ctx: TSetContext):
    x_values = ctx.get_input("x").consumer()
    for x, z in zip(itr, x_values):
        collector.collect(x + z)


calc = source_z.compute(combine_x_and_z)
calc.add_input("x", cached)

calc.for_each(lambda i: print("(x * 5) + 2 + z = %d" % i))
