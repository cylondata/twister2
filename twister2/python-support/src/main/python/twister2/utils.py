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


def function_wrapper(lam):
    """
    This functions wraps a user defined function to support argument types conversion
    :param lam: user defined function
    :return: wrapped function
    """

    def func_wrapper(*args):
        new_args = do_args_conversion(*args)
        return lam(*new_args)

    return func_wrapper


def do_arg_map(arg):
    type_str = str(type(arg))
    if type_str == "<class 'jep.PyJList'>":
        return list(arg)
    elif type_str == "<class 'jep.PyJIterator'>":
        return IteratorWrapper(arg)
    return arg


def do_args_conversion(*args):
    converted_args = map(do_arg_map, args)
    return converted_args


def call_function(name, *args):
    print("executing %s" % name)
    print(args)
    print(globals())
    return globals()[name](*args)
