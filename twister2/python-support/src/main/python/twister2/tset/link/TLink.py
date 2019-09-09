from twister2.utils import do_args_conversion


class TLink:

    def __init__(self, java_ref, env):
        self.__java_ref = java_ref
        self.__env = env

    def sink(self, sink_func):
        def sink_wrapper(*args):
            new_args = do_args_conversion(*args)
            return sink_func(*new_args)

        sink_func_java_ref = self.__env.functions.sink.build(sink_wrapper)
        self.__java_ref.sink(sink_func_java_ref)
