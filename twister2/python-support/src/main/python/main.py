from twister2.Twister2Environment import Twister2Environment

ctx = Twister2Environment()

print("Hello from python worker %d" % ctx.worker_id)


class IntegerSource:

    def __init__(self):
        self.x = 0

    def has_next(self):
        return self.x < 100

    def next(self):
        self.x += 1
        return self.x


int_source = IntegerSource()

source = ctx.create_source(int_source, 4)

source.direct()



