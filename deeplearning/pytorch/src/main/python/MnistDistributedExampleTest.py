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

from twister2deepnet.deepnet.examples.MnistDistributed import MnistDistributed

env = Twister2Environment(resources=[{"cpu": 1, "ram": 512, "instances": 4}])

comm = MPI.COMM_WORLD

world_size = int(os.environ['OMPI_COMM_WORLD_SIZE'])
world_rank = int(os.environ['OMPI_COMM_WORLD_RANK'])

mniste = MnistDistributed(parallelism=world_size, rank=world_rank)

train_x, train_y, batch_size = mniste.load_data()
print(type(train_x), type(train_y), batch_size, env.worker_id, world_rank, world_size)