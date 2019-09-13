import cloudpickle as cp


class ComputeFunctions:

    def __init__(self, java_ref, env):
        self.__java_ref = java_ref
        self.__env = env

    def build(self, compute_func):
        # send python dump to java -> create a java object in JVM -> get the ref back
        return self.__java_ref.build(cp.dumps(compute_func))
