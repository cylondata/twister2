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

import mpi4py

mpi4py.rc(initialize=False, finalize=False)
from mpi4py import MPI

from twister2.Twister2Environment import Twister2Environment

env = Twister2Environment(resources=[{"cpu": 1, "ram": 512, "instances": 4}])

# TODO: expose MPI_COMM_WORLD from Java API

comm = MPI.COMM_WORLD

# Your worker code starts here
print("Hello from worker {} {}".format(env.worker_id, comm) )

parent = comm.Get_parent()
world_rank = comm.Get_rank()
world_size = comm.Get_size()
