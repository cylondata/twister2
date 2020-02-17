class DataPartition:

    def __init__(self, java_ref, env):
        self.__java_ref = java_ref
        self.__env = env

    @property
    def id(self):
        return self.__java_ref.getPartitionId()

    def consumer(self):
        from twister2.utils import IteratorWrapper
        return IteratorWrapper(self.__java_ref.getConsumer(), self.__env.numpy_builder)

    def first(self):
        from twister2.utils import do_arg_map
        return do_arg_map(self.__java_ref.first())

    def first_or_default(self, default):
        first = self.first()
        if first is None:
            return default
        return first
