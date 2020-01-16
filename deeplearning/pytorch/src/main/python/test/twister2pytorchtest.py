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
# TWISTER2 IMPORTS
from twister2.Twister2Environment import Twister2Environment
from twister2.tset.fn.SourceFunc import SourceFunc
# MPI IMPORTS
import mpi4py
mpi4py.rc(initialize=False, finalize=False)
from mpi4py import MPI
# NUMPY IMPORTS
import numpy as np

env = Twister2Environment(resources=[{"cpu": 1, "ram": 512, "instances": 4}])

print("Hello from worker %d" % env.worker_id)


class IntSource(SourceFunc):

    def __init__(self):
        super().__init__()
        self.i = 0

    def has_next(self):
        return self.i < 10

    def next(self):
        res = self.i
        self.i = self.i + 1
        return self.generator()

    def generator(self):
        self.data = np.random.rand(1,5)
        return self.data

comm = MPI.COMM_WORLD
world_size = int(os.environ['OMPI_COMM_WORLD_SIZE'])
world_rank = int(os.environ['OMPI_COMM_WORLD_RANK'])

source_x = env.create_source(IntSource(), world_size)

source_x.for_each(lambda i: print(i, world_rank, world_size))
