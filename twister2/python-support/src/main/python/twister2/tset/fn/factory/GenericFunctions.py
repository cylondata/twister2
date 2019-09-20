import cloudpickle as cp


class GenericFunctions:

    def __init__(self, java_ref, env):
        self.__java_ref = java_ref
        self.__env = env

    def build(self, compute_func):
        """
        send python dump to java -> create a java object in JVM -> get the ref back
        :param compute_func: user defined compute function
        :return: java reference to the compute function
        """
        return self.__java_ref.build(cp.dumps(compute_func))
