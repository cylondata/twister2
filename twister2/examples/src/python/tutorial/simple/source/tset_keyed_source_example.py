from twister2.Twister2Environment import Twister2Environment
from twister2.tset.fn.SourceFunc import SourceFunc

env = Twister2Environment(resources=[{"cpu": 1, "ram": 512, "instances": 4}])

print("Hello from worker %d" % env.worker_id)


class KeyedIntSource(SourceFunc):

    def __init__(self):
        super().__init__()
        self.i = 0

    def has_next(self):
        return self.i < 10

    def next(self):
        res = self.i
        self.i = self.i + 1
        return res % 3, res


source_x = env.create_keyed_source(KeyedIntSource(), 4)

source_x.keyed_gather(key_comparator=lambda a, b: a - b,
                      partition_func=env.functions.partition.load_balanced, use_disk=True) \
    .for_each(lambda x: print(x))
