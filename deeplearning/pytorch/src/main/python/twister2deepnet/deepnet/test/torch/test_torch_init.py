import mpi4py

mpi4py.rc(initialize=False, finalize=False)

from mpi4py import MPI

import torch.distributed as dist1

MPI.Init()

comm = MPI.COMM_WORLD

rank = comm.Get_rank()
size = comm.Get_size()

dist1.init_process_group("mpi", rank=rank, world_size=size)

print("Rank {} Size {}".format(rank, size))

MPI.Finalize()




