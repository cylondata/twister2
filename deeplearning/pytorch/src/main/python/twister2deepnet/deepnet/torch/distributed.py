import torch
import mpi4py
import numpy as np

mpi4py.rc(initialize=False, finalize=False)
from mpi4py import MPI
from twister2deepnet.deepnet.torch.ReduceOp import ReduceOp


def init_process_group():
    if not MPI.Is_initialized():
        MPI.Init()
    global _comm
    _comm = MPI.COMM_WORLD


def finalize_process_group():
    if not MPI.Is_finalized():
        MPI.Finalize()


def _get_comm():
    return MPI.COMM_WORLD


def get_comm():
    return _get_comm()


def get_rank(comm: MPI.COMM_WORLD) -> int:
    return comm.Get_rank()


def get_size(comm: MPI.COMM_WORLD) -> int:
    return comm.Get_size()


def all_reduce(tensor: torch.Tensor, op=ReduceOp.SUM, comm: MPI.COMM_WORLD = None) -> torch.Tensor:
    param_numpy = tensor.numpy()
    param_output = np.empty(param_numpy.shape, dtype=param_numpy.dtype)
    if comm is None:
        comm = _get_comm()
    comm.Allreduce(param_numpy, param_output, op=op.value)
    out_tensor: torch.Tensor = torch.from_numpy(param_output)
    return out_tensor
