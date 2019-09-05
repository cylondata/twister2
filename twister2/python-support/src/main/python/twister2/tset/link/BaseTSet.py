class BaseTSet:

    def __init__(self, java_ref, env):
        self.__java_ref = java_ref
        self.__env = env

    @property
    def parallelism(self):
        return self.__java_ref.getParallelism()

    @property
    def name(self):
        return self.__java_ref.getName()

    @property
    def id(self):
        return self.__java_ref.getId()
