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
import time
# TWISTER2 IMPORTS
from twister2.Twister2Environment import Twister2Environment
from twister2.tset.fn.SourceFunc import SourceFunc
# MPI IMPORTS
import mpi4py
mpi4py.rc(initialize=False, finalize=False)
from mpi4py import MPI
# NUMPY IMPORTS
import numpy as np
from math import ceil

from twister2deepnet.deepnet.examples.MnistDistributed import MnistDistributed
from twister2deepnet.deepnet.data.DataPartitioner import DataPartitioner
from twister2deepnet.deepnet.network.MnistNet import  MnistNet

import torch
import torch.optim as optim
import torch.nn.functional as F

#MPI.Init()
comm = MPI.COMM_WORLD
device = torch.device("cpu")
print("CommWorld : ", comm)


env = Twister2Environment(resources=[{"cpu": 1, "ram": 512, "instances": 4}])



world_size = int(os.environ['OMPI_COMM_WORLD_SIZE'])
world_rank = int(os.environ['OMPI_COMM_WORLD_RANK'])


mniste = MnistDistributed(parallelism=world_size, rank=world_rank)

train_set_data, train_set_target, bsz =  mniste.load_data()

print(type(train_set_data), type(train_set_target), bsz, env.worker_id, world_rank,
      world_size)

#model = MnistNet()
#optimizer = optim.SGD(model.parameters(), lr=0.01, momentum=0.5)

#num_batches = ceil(len(train_set_data.dataset) / float(bsz))

# if (world_rank == 0):
#     print("Started Training")
# total_data = len(train_set_data)
# epochs = 20
# total_steps = epochs * total_data
# local_time_communication = 0
# local_total_time_communication = 0

# for epoch in range(epochs):
#     epoch_loss = 0.0
#     count = 0
#     for data, target in zip(train_set_data, train_set_target):
#         data = np.reshape(data, (data.shape[0], 1, data.shape[1], data.shape[2])) / 128.0
#         # print(
#         #     "Data Size {}({},{}) of Rank {} : target {}, {}".format(data.shape, (data[0].numpy().dtype), type(data),
#         #                                                             world_rank, target, len(target)))
#         # print(data[0], target[0])
#         count = count + 1
#         result = '{0:.4g}'.format((count / float(total_steps)) * 100.0)
#         if (world_rank == 0):
#             print("Progress {}% \r".format(result), end='\r')
#         optimizer.zero_grad()
#         output = model(data)
#         loss = F.nll_loss(output, target)
#         epoch_loss += loss.item()
#         # print(epoch_loss)
#         loss.backward()
#         if (world_rank == 0):
#             local_time_communication = time.time()
#         #average_gradients_mpi(model, comm=comm, world_size=world_size)
#         if (world_rank == 0):
#             local_time_communication = time.time() - local_time_communication
#             local_total_time_communication = local_total_time_communication + local_time_communication
#         optimizer.step()
#     if (world_rank == 0):
#         print('Rank ', world_rank, ', epoch ',
#               epoch, ': ', epoch_loss / num_batches)


#MPI.Finalize()