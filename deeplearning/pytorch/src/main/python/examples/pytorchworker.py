#  // Licensed under the Apache License, Version 2.0 (the "License");
#  // you may not use this file except in compliance with the License.
#  // You may obtain a copy of the License at
#  //
#  // http://www.apache.org/licenses/LICENSE-2.0
#  //
#  // Unless required by applicable law or agreed to in writing, software
#  // distributed under the License is distributed on an "AS IS" BASIS,
#  // WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  // See the License for the specific language governing permissions and
#  // limitations under the License.

import os
import socket
import torch
import torch.distributed as dist


def run(rank, size, hostname):
    print(f"I am {rank} of {size} in {hostname}")
    tensor1 = torch.zeros(4)
    if rank == 0:
        # Send the tensor to process 1
        tensor_send = torch.ones(4) * 10
        dist.send(tensor=tensor_send, dst=1)
    else:
        # Receive tensor from process 0
        dist.recv(tensor=tensor1, src=0)

    print('Rank ', rank, ' has data ', tensor1)


def init_processes(rank, size, hostname, fn, backend='tcp'):
    """ Initialize the distributed environment. """
    dist.init_process_group(backend, rank=rank, world_size=size)
    fn(rank, size, hostname)


world_size = int(os.environ['OMPI_COMM_WORLD_SIZE'])
world_rank = int(os.environ['OMPI_COMM_WORLD_RANK'])
hostname = socket.gethostname()
init_processes(world_rank, world_size, hostname, run, backend='mpi')