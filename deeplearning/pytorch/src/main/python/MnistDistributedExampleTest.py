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
import pandas as pd
from math import ceil

from twister2deepnet.deepnet.examples.MnistDistributed import MnistDistributed
from twister2deepnet.deepnet.network.MnistNet import  MnistNet
from twister2deepnet.deepnet.data.UtilPanda import UtilPanda
from twister2deepnet.deepnet.io.FileUtils import FileUtils
from twister2deepnet.deepnet.io.ArrowUtils import ArrowUtils

import torch
import torch.optim as optim
import torch.nn.functional as F

#MPI.Init()
comm = MPI.COMM_WORLD
device = torch.device("cpu")
print("CommWorld : ", comm)

###############################################
########## Twister2 Data Processing ##########
##############################################

env = Twister2Environment(resources=[{"cpu": 1, "ram": 512, "instances": 4}])

world_size = int(os.environ['OMPI_COMM_WORLD_SIZE'])
world_rank = int(os.environ['OMPI_COMM_WORLD_RANK'])

train_data_save_path = "/tmp/parquet/train/"
test_data_save_path = "/tmp/parquet/test/"
train_data_file = str(world_rank)

if world_rank == 0:
    FileUtils.mkdir(train_data_save_path)
    FileUtils.mkdir(test_data_save_path)

mniste = MnistDistributed(parallelism=world_size, rank=world_rank)

utilPanda = UtilPanda()

train_set_data, train_set_target, bsz =  mniste.load_data()
dataset = train_set_data.dataset

dataframe = utilPanda.convert_to_pandas(dataset)

table = ArrowUtils.create_to_table(dataFrame=dataframe)

ArrowUtils.write_to_table(table=table,save_path=train_data_save_path + train_data_file)

print(type(dataset), len(dataset), dataframe.shape, world_rank, world_size, type(table))


###############################################
########## Pytorch Data Processing ##########
##############################################

