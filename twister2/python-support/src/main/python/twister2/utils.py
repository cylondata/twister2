import numpy as np

from twister2.TSetContext import TSetContext


class IteratorWrapper:
    def __init__(self, java_ref):
        self.__java_ref = java_ref

    def __iter__(self):
        return self

    def __next__(self):
        if self.__java_ref.hasNext():
            return do_arg_map(self.__java_ref.next())
        else:
            raise StopIteration()


class SourceWrapper:

    def __init__(self, source_func):
        self.__source_func = source_func

    def has_next(self, **kwargs):
        return self.__source_func.has_next()

    def next(self, **kwargs):
        next_data = self.__source_func.next()
        return do_arg_map(next_data, kwargs["numpy_builder"])


def function_wrapper(lam):
    """
    This functions wraps a user defined function to support argument types conversion.
    :param lam: user defined function
    :return: wrapped function
    """

    def func_wrapper(*args, **kwargs):
        new_args = do_args_conversion(*args)
        return do_arg_map(lam(*new_args), kwargs["numpy_builder"])

    return func_wrapper


def do_arg_map(arg, numpy_builder=None):
    type_str = str(type(arg))
    if type_str == "<class 'jep.PyJList'>":  # J to P
        return list(arg)
    elif type_str == "<class 'jep.PyJIterator'>":  # J to P
        return IteratorWrapper(arg)
    elif type_str == "<class 'numpy.ndarray'>":  # P to J
        return numpy_builder.build(arg.tolist(), arg.shape, arg.dtype)
    elif type_str == "<class 'jep.PyJObject'>":  # J to P
        if hasattr(arg, "getNumpy"):
            # numpy array found
            return np.array(arg.getNumpy())
        if hasattr(arg, "getInput"):
            # TSetContext
            return TSetContext(arg)
    elif arg is None:
        return None

    return arg


def do_args_conversion(*args):
    converted_args = map(do_arg_map, args)
    return converted_args


def call_function(name, *args):
    print("executing %s" % name)
    print(args)
    print(globals())
    return globals()[name](*args)
