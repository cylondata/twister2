import sys

import cloudpickle as cp
from py4j.java_gateway import JavaGateway, GatewayParameters

from twister2.tset.SourceTSet import SourceTSet
from twister2.tset.fn.SourceFunc import SourceFunc
from twister2.tset.fn.factory.TSetFunctions import TSetFunctions


class Twister2Environment:

    def __init__(self):
        print("Connecting to java port %s" % sys.argv[1])
        gateway = JavaGateway(gateway_parameters=GatewayParameters(port=int(sys.argv[1])))
        self.__entrypoint = gateway.entry_point
        self.__predef_functions = TSetFunctions(self.__entrypoint.functions(), self)

    @property
    def config(self):
        return self.__entrypoint.getConfig()

    @property
    def worker_id(self):
        return self.__entrypoint.getWorkerId()

    @property
    def functions(self):
        return self.__predef_functions

    def create_source(self, source_function, parallelism=0) -> SourceTSet:
        if not isinstance(source_function, SourceFunc):
            raise Exception('source_function should be an instance of {}'.format(SourceFunc))

        java_src_ref = self.__entrypoint.createSource(cp.dumps(source_function), parallelism)
        src_tset = SourceTSet(java_src_ref, self)
        return src_tset
