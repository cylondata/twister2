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

# NUMPY IMPORTS
import numpy as np
import os
import torch
import torch.nn as nn
import torch.nn.functional as F
import time
from math import sqrt
from math import ceil

import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim

import os
import torch
import torch.distributed as dist

from twister2deepnet.deepnet.io.ArrowUtils import ArrowUtils


class Net(nn.Module):

    def __init__(self):
        super(Net, self).__init__()
        self.conv1 = nn.Conv2d(1, 32, 3, 1)
        self.conv2 = nn.Conv2d(32, 64, 3, 1)
        self.dropout1 = nn.Dropout2d(0.25)
        self.dropout2 = nn.Dropout2d(0.5)
        self.fc1 = nn.Linear(9216, 128)
        self.fc2 = nn.Linear(128, 10)

    def forward(self, x):
        x = self.conv1(x)
        x = F.relu(x)
        x = self.conv2(x)
        x = F.max_pool2d(x, 2)
        x = self.dropout1(x)
        x = torch.flatten(x, 1)
        x = self.fc1(x)
        x = F.relu(x)
        x = self.dropout2(x)
        x = self.fc2(x)
        output = F.log_softmax(x, dim=1)
        return output

""" Gradient averaging. """


def average_gradients(model):
    size = float(dist.get_world_size())
    for param in model.parameters():
        dist.all_reduce(param.grad.data, op=dist.ReduceOp.SUM)
        param.grad.data /= size


def average_accuracy(local_accuracy):
    size = float(dist.get_world_size())
    dist.all_reduce(local_accuracy, op=dist.ReduceOp.SUM)
    global_accuracy = local_accuracy / size
    return global_accuracy

def init_processes(rank, size, fn, backend='tcp',
                   train_data=None, train_target=None,
                   test_data=None, test_target=None,
                   do_log=False):
    """ Initialize the distributed environment. """
    dist.init_process_group(backend, rank=rank, world_size=size)
    use_cuda = torch.cuda.is_available()
    # device = torch.device("cuda" if use_cuda else "cpu")
    device = torch.device("cpu")
    print(rank, size)
    fn(world_rank=rank, world_size=size, train_data=train_data,
       train_target=test_data, do_log=False)
    # model1 = Net()
    # test(rank, model1, device)
    total_communication_time = 0
    # local_training_time = 0
    # local_testing_time = 0
    # if (rank == 0):
    #     local_training_time = time.time()
    # model, total_communication_time = fn(rank, size)
    # if (rank == 0):
    #     local_training_time = time.time() - local_training_time
    # if (rank == 0):
    #     local_testing_time = time.time()
    # test(rank, model, device, do_log=do_log)
    # if (rank == 0):
    #     local_testing_time = time.time() - local_testing_time
    #     print("Total Training Time : {}".format(local_training_time))
    #     print("Total Testing Time : {}".format(local_testing_time))
    #     save_log("stats.csv",
    #              stat="{},{},{},{}".format(size, local_training_time, total_communication_time, local_testing_time))


def run(world_rank=0, world_size=4, train_data=None, train_target=None, do_log=False):
    if (world_rank == 0):
        print("Run Fn")

    torch.manual_seed(1234)
    bsz = int(128 / float(world_size))
    #train_set_data, train_set_target, bsz = partition_numpy_dataset()

    model = Net()
    optimizer = optim.SGD(model.parameters(),
                          lr=0.01, momentum=0.5)

    num_batches = ceil(len(train_data) / float(bsz))

    if (world_rank == 0):
        print("Started Training")
    total_data = len(train_data)
    epochs = 10
    total_steps = epochs * total_data
    local_time_communication = 0
    local_total_time_communication = 0

    for epoch in range(20):
        epoch_loss = 0.0
        count = 0
        for data, target in zip(train_data, train_target):
            print("batch_size {}, data shape {} ".format(bsz, data.shape))
            data = np.reshape(data, (data.shape[0], 1, data.shape[1], data.shape[2])) / 128.0
            # print(
            #     "Data Size {}({},{}) of Rank {} : target {}, {}".format(data.shape, (data[0].numpy().dtype), type(data),
            #                                                             rank, target, len(target)))
            # print(data[0], target[0])
            count = count + 1
            result = '{0:.4g}'.format((count / float(total_steps)) * 100.0)
            if (world_rank == 0):
                print("Progress {}% \r".format(result), end='\r')
            optimizer.zero_grad()
            output = model(data)
            loss = F.nll_loss(output, target)
            epoch_loss += loss.item()
            # print(epoch_loss)
            loss.backward()
            if (world_rank == 0):
                local_time_communication = time.time()
            average_gradients(model)
            if (world_rank == 0):
                local_time_communication = time.time() - local_time_communication
                local_total_time_communication = local_total_time_communication + local_time_communication
            optimizer.step()
        if (world_rank == 0):
            print('Rank ', dist.get_rank(), ', epoch ',
                  epoch, ': ', epoch_loss / num_batches)
    return model, local_total_time_communication


def format_data(input_data=None, world_size=4, init_batch_size=128):
    """
    Specific For MNIST and 3 dimensional data
    This function is generated for MNIST only cannot be used in general for all data shapes
    :param input_data: data in numpy format with (N,M) format Here N number of samples
            M is the tensor length
    :return: For numpy we reshape this and return a tensor of the shape, (N, sqrt(M), sqrt(M))
    """
    bsz = int(init_batch_size / float(world_size))
    data_shape = input_data.shape
    num_batches = ceil(data_shape[0] / float(bsz))
    """
        TODO: NOW write a custom class or method to do the following:
            Look total data and create mini-batches
            Each mini-batch has a user-defined batch size
            For instance 15,000 data divided for batch size 32
            There can 469 partitions. 
            But we cannot use re-shape
            create 468 * 32 = 14976 ( 468 batches)
            and create the 469 batch with re-using already used values 
            with the remaining values 15000-14976=24
            So adding 8 extra values from existing data 
            do this randomly
            take a look at the code of Pytorch how they do it.  
    """

    data = np.reshape(input_data, (data_shape[0], int(sqrt(data_shape[1])), int(sqrt(data_shape[1]))))
    print("format : ",data_shape, num_batches, data.shape, bsz)
    return data


def read_from_disk(source_file=None, source_path=None):
    """
    A helper function to load the data from the disk
    This function is useful for in-memory oriented data reads, doesn't support very large data reads
    Reads the data from disk (using PyArrow Parquet)
    :param source_file: file name
    :param source_path: path to reading file
    :return: returns a numpy array of the saved data
    """
    data = None
    if source_file is None and source_path is None:
        raise Exception("Input cannot be None")
    elif not os.path.exists(source_path + source_file):
        raise Exception("Data source doesn't exist")
    else:
        dataframe = ArrowUtils.read_from_table(source_path + source_file)
        data = dataframe.to_numpy()
    return data


world_size = int(os.environ['OMPI_COMM_WORLD_SIZE'])
world_rank = int(os.environ['OMPI_COMM_WORLD_RANK'])

train_data_save_path = "/tmp/parquet/train/"
test_data_save_path = "/tmp/parquet/test/"

train_data_file = str(world_rank) + ".data"
test_data_file = str(world_rank) + ".data"
train_target_file = str(world_rank) + ".target"
test_target_file = str(world_rank) + ".target"


# LOAD DATA FROM DISK

## load train data
train_data = read_from_disk(source_file=train_data_file, source_path=train_data_save_path)
train_data = format_data(input_data=train_data, world_size=world_size, init_batch_size=128)
## load test data
test_data = read_from_disk(source_file=test_data_file, source_path=test_data_save_path)
test_data = format_data(input_data=test_data, world_size=world_size, init_batch_size=16 )
## load train target
train_target = read_from_disk(source_file=train_target_file, source_path=train_data_save_path)
train_target = format_data(input_data=train_target, world_size=world_size, init_batch_size=128)
## load test target
test_target = read_from_disk(source_file=test_target_file, source_path=test_data_save_path)
test_target = format_data(input_data=test_target, world_size=world_size, init_batch_size=16)

#print(train_data.shape, train_target.shape, test_data.shape, test_target.shape)

do_log = True

# initialize training
# init_processes(rank=world_rank, size=world_size, fn=run, backend='mpi',
#                train_data=train_data, train_target=train_target,
#                test_data=test_data, test_target=test_target,
#                do_log=do_log)

