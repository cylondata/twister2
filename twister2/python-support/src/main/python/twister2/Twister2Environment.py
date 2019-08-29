import sys

import cloudpickle as cp
from py4j.java_gateway import JavaGateway, GatewayParameters
from twister2.tset.SourceTSet import SourceTSet


class Twister2Environment:

    def __init__(self):
        print("Connecting to java port %s" % sys.argv[1])
        gateway = JavaGateway(gateway_parameters=GatewayParameters(port=int(sys.argv[1])))
        self.entrypoint = gateway.entry_point

    def sout(self):
        self.entrypoint.sout()

    @property
    def worker_id(self):
        return self.entrypoint.getWorkerId()

    def execute_function(self, lam, data=None):
        self.entrypoint.executePyFunction(cp.dumps(lam), data)

    def execute_obj(self, lam, data=None):
        self.entrypoint.executePyObject(cp.dumps(lam), data)

    def create_source(self, lam, parallelism=0) -> SourceTSet:
        java_src_ref = self.entrypoint.createSource(cp.dumps(lam), parallelism)
        src_tset = SourceTSet(java_src_ref)
        return src_tset
