import numpy as np
import mpi4py
import torch

mpi4py.rc(initialize=False, finalize=False)
from mpi4py import MPI

MPI.Init()

comm = MPI.COMM_WORLD
parent = comm.Get_parent()
world_rank = parent.Get_rank()
world_size = parent.Get_size()

recv_data = np.array([0, 0, 0, 0], dtype="i")
if world_rank == 1:
    parent.Recv([recv_data, MPI.INT], source=0, tag=0)

print("From Slave: ", world_rank, world_size, parent, recv_data)
#comm.Recv([recv_data, MPI.INT], source=0)

tensor1 = torch.from_numpy(recv_data)
tensor2 = torch.from_numpy(np.ones(4))
tensor3 = tensor1 + tensor2

print("Results : ", tensor3)

# comm.free()
MPI.Finalize()
