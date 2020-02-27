import torch
import twister2deepnet.deepnet.torch.distributed as dist
from twister2deepnet.deepnet.torch.ReduceOp import ReduceOp

dist.init_process_group()

comm = dist.get_comm()

world_rank: int = dist.get_rank(comm)
world_size: int = dist.get_size(comm)

print("World Rank {}, World Size {}".format(world_rank, world_size))

local = torch.tensor([0, 1, 2, 3]) * world_rank

global_val = dist.all_reduce(local, op=ReduceOp.SUM)

print(local, global_val)

dist.finalize_process_group()
