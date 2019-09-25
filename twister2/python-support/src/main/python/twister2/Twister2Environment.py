import cloudpickle as cp
import os
import sys
import time
from py4j.java_gateway import JavaGateway, GatewayParameters

from twister2.tset.TSet import TSet
from twister2.tset.fn.SourceFunc import SourceFunc
from twister2.tset.fn.factory.TSetFunctions import TSetFunctions
from twister2.utils import SourceWrapper


class Twister2Environment:

    def __init__(self, name=None, resources=None, config=None):
        if config is None:
            config = {}
        if resources is None:
            resources = []

        port = int(os.environ['T2_PORT'])
        bootstrap = os.environ['T2_BOOTSTRAP'] == "true"
        gateway = JavaGateway(gateway_parameters=GatewayParameters(port=port))
        self.__entrypoint = gateway.entry_point

        if bootstrap:
            for key in config:
                self.__entrypoint.addConfig(key, config[key])

            if name is not None:
                self.__entrypoint.setJobName(name)

            for resource in resources:
                self.__entrypoint.createComputeResource(resource["cpu"], resource["ram"], resource["instances"])

            self.__entrypoint.commit()
            time.sleep(5)
            gateway.shutdown()
            sys.exit(0)
        else:
            self.__predef_functions = TSetFunctions(self.__entrypoint.functions(), self)

    @property
    def config(self):
        return self.__entrypoint.getConfig()

    @property
    def worker_id(self):
        return self.__entrypoint.getWorkerId()

    @property
    def functions(self) -> TSetFunctions:
        return self.__predef_functions

    def create_source(self, source_function: SourceFunc, parallelism=0) -> TSet:
        if not isinstance(source_function, SourceFunc):
            raise Exception('source_function should be an instance of {}'.format(SourceFunc))

        source_function_wrapper = SourceWrapper(source_function)

        java_src_ref = self.__entrypoint.createSource(cp.dumps(source_function_wrapper), parallelism)
        src_tset = TSet(java_src_ref, self)
        return src_tset
