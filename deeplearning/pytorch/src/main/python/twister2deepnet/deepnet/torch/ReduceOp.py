from mpi4py import MPI
from enum import Enum


class ReduceOp(Enum):
    SUM = MPI.SUM
    MAX = MPI.MAX
    MIN = MPI.MIN
    PROD = MPI.PROD
