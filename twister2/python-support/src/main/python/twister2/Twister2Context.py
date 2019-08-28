from py4j.java_gateway import JavaGateway, GatewayParameters
import sys
import cloudpickle as cp


class Twister2Context:

    def __init__(self, gateway):
        self.gateway = gateway
        self.entrypoint = gateway.entry_point

    def sout(self):
        self.entrypoint.sout()

    @property
    def worker_id(self):
        return self.entrypoint.getWorkerId()

    def execute(self, lam, data):
        self.entrypoint.executePyFunction(cp.dumps(lam), data)

    @staticmethod
    def init():
        print("Connecting to java port %s" % sys.argv[1])
        gateway = JavaGateway(gateway_parameters=GatewayParameters(port=int(sys.argv[1])))
        return Twister2Context(gateway)
