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
import numpy as np
# CORE PYTWISTER2 IMPORTS
from twister2 import TSetContext
from twister2.Twister2Environment import Twister2Environment
from twister2.tset.TSet import TSet
from twister2.tset.fn.SourceFunc import SourceFunc
# TWISTER2 DEEPNET IMPORTS
from twister2deepnet.deepnet.data.UtilPanda import UtilPanda
from twister2deepnet.deepnet.examples.MnistDistributed import MnistDistributed
from twister2deepnet.deepnet.io.ArrowUtils import ArrowUtils
from twister2deepnet.deepnet.io.FileUtils import FileUtils
from twister2deepnet.deepnet.data.DataUtil import DataUtil

import torch
import torch.distributed as dist
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from math import sqrt

import mpi4py
from mpi4py import MPI

import time

DATA_FOLDER = '/tmp/twister2deepnet/mnist/'

TRAIN_DATA_SAVE_PATH = "/tmp/parquet/train/"
TEST_DATA_SAVE_PATH = "/tmp/parquet/test/"

PARALLELISM = 4

env = Twister2Environment(resources=[{"cpu": 1, "ram": 2048, "instances": PARALLELISM}], mpi_aware=True)

comm = MPI.COMM_WORLD

world_size = PARALLELISM  # int(os.environ['OMPI_COMM_WORLD_SIZE'])
world_rank = env.worker_id

TRAIN_DATA_FILE = str(world_rank) + ".data"
TRAIN_TARGET_FILE = str(world_rank) + ".target"
TEST_DATA_FILE = str(world_rank) + ".data"
TEST_TARGET_FILE = str(world_rank) + ".target"

TRAIN_DATA_FILES = [TRAIN_DATA_FILE, TRAIN_TARGET_FILE]
TEST_DATA_FILES = [TEST_DATA_FILE, TEST_TARGET_FILE]
DATA_SAVE_PATHS = [TRAIN_DATA_SAVE_PATH, TEST_DATA_SAVE_PATH]

if env.worker_id == 0:
    FileUtils.mkdir_branch_with_access(TRAIN_DATA_SAVE_PATH)
    FileUtils.mkdir_branch_with_access(TEST_DATA_SAVE_PATH)


# print("Hello from worker %d" % env.worker_id)

class DataSource(SourceFunc):

    def __init__(self, train=True):
        super().__init__()
        self.is_preprocess = True
        self.is_loaded = False
        self.mniste = None
        self.train_dataset = None
        self.train_targetset = None
        self.test_dataset = None
        self.test_targetset = None
        self.train_bsz = None
        self.test_bsz = None
        self.train_data_load = []
        self.test_data_load = []
        self.data_load = []
        self.i = 0
        self.train = train
        self.message_size = 10
        self.load_data()

    def has_next(self):
        return self.i < len(self.data_load)

    def next(self):
        res = self.data_load[self.i]
        self.i = self.i + 1
        # TODO: packaging a message with meta
        # message = np.array([[self.i], [res]])
        return res

    def load_data(self):
        if not self.is_loaded:
            self.mniste = MnistDistributed(source_dir=DATA_FOLDER, parallelism=world_size,
                                           world_rank=world_rank)
            if self.train:
                self.train_dataset, self.train_targetset, self.train_bsz = self.mniste.train_data
                self.data_load.append(self.train_dataset)
                self.data_load.append(self.train_targetset)
            else:
                self.test_dataset, self.test_targetset, self.test_bsz = self.mniste.test_data
                self.data_load.append(self.test_dataset)
                self.data_load.append(self.test_targetset)
            self.is_loaded = True
        else:
            pass


def save_to_disk(dataset=None, save_path=None, save_file=None):
    # TODO use os.path.join and refactor
    if dataset is None or save_path is None or save_file is None:
        raise Exception("Input Cannot be None")
    elif not os.path.exists(save_path):
        raise Exception("Save Path doesn't exist")
    elif os.path.exists(save_path + save_file):
        pass
    else:
        dataframe = UtilPanda.convert_numpy_to_pandas(dataset)
        table = ArrowUtils.create_to_table(dataFrame=dataframe)
        ArrowUtils.write_to_table(table=table, save_path=os.path.join(save_path, save_file))


def tset_to_numpy(tset):
    cache_tset = tset.cache()
    test_data_object = cache_tset.get_data()
    data_items = []
    for partition in test_data_object.get_partitions():
        for consumer in partition.consumer():
            data_items.append(consumer)
    return data_items


def check_one_dim_array(arr: np.ndarray):
    return True if len(arr.shape) == 1 else False


def fix_array_shape(arr: np.ndarray):
    if check_one_dim_array(arr):
        arr = np.reshape(arr, (arr.shape[0], 1))
    return arr


def load_data():
    t1 = time.time()

    source_train = env.create_source(DataSource(train=True), PARALLELISM)
    source_train_tset_cache = source_train.cache()

    source_test = env.create_source(DataSource(train=False), PARALLELISM)
    source_test_tset_cache = source_test.cache()

    data_loading_time = time.time() - t1

    t1 = time.time()

    train_data_list = tset_to_numpy(source_train)
    test_data_list = tset_to_numpy(source_test)

    convert_numpy_time = time.time() - t1

    print("Source Test {} {} {} {}".format(len(test_data_list), type(test_data_list[0]), test_data_list[0].shape,
                                           test_data_list[1].shape))

    time_taken = data_loading_time + convert_numpy_time

    print("Total Time Taken {}, \nData Loading Time {}, Numpy Conversion Time {} ".format(time_taken, data_loading_time,
                                                                                          convert_numpy_time))
    train_data = train_data_list[0]
    train_target = train_data_list[1]

    test_data = test_data_list[0]
    test_target = test_data_list[1]
    return train_data, train_target, test_data, test_target


dtype = np.float32

samples = 8
features = 784

train_data, train_target, test_data, test_target = load_data()

train_target = fix_array_shape(train_target)
test_target = fix_array_shape(test_target)

if world_rank == 0:
    print("From Memory: ", train_data.shape, train_target.shape, test_data.shape, test_target.shape)

print("Loading Done")

##############################################################################

print("Start Training")

import numpy as np
import os
import time
import torch
import torch.distributed as dist
import torch.nn as nn
import torch.nn.functional as F
import torch.optim as optim
from math import sqrt

from twister2deepnet.deepnet.data.DataUtil import DataUtil
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


def average_gradients_mpi(model, comm=None, world_size=4):
    size = float(world_size)
    for param in model.parameters():
        param_numpy = param.grad.data.numpy()
        param_output = np.empty(param_numpy.shape, dtype=param_numpy.dtype)
        param_torch = torch.from_numpy(param_numpy)
        # print(type(param.grad.data), type(param_numpy), type(param_torch), param_numpy.dtype, param_output.dtype)
        comm.Allreduce(param_numpy, param_output, op=MPI.SUM)
        param.grad.data = torch.from_numpy(param_output)
        # dist.all_reduce(param.grad.data, op=dist.ReduceOp.SUM)
        param.grad.data /= size


def average_accuracy_mpi(local_accuracy, comm=None, world_size=4):
    size = float(world_size)
    # dist.all_reduce(local_accuracy, op=dist.ReduceOp.SUM)
    param_numpy = local_accuracy.numpy()
    param_output = np.empty(param_numpy.shape, dtype=param_numpy.dtype)
    comm.Allreduce(param_numpy, param_output, op=MPI.SUM)
    local_accuracy = torch.from_numpy(param_output)
    global_accuracy = local_accuracy / size
    return global_accuracy


def save_log(file_path=None, stat=""):
    """
    saving the program timing stats
    :rtype: None
    """
    fp = open(file_path, mode="a+")
    fp.write(stat + "\n")
    fp.close()


def launch(rank, size, fn, backend='tcp',
           train_data=None, train_target=None,
           test_data=None, test_target=None,
           do_log=False,
           comms=None):
    """ Initialize the distributed environment.
    :param rank: process id (MPI world rank)
    :param size: number of processes (MPI world size)
    :param fn: training function
    :param backend: Pytorch Backend
    :param train_data: training data
    :param train_target: training targets
    :param test_data: testing data
    :param test_target: testing targets
    :param do_log: boolean status to log
    """
    # dist.init_process_group(backend, rank=rank, world_size=size)
    # Setting CUDA FOR TRAINING
    # use_cuda = torch.cuda.is_available()
    # device = torch.device("cuda" if use_cuda else "cpu")

    device = torch.device("cpu")

    total_communication_time = 0
    local_training_time = 0
    local_testing_time = 0
    if (rank == 0):
        local_training_time = time.time()

    model, total_communication_time = fn(world_rank=rank, world_size=size, train_data=train_data,
                                         train_target=train_target, do_log=False, comms=comms)
    if (rank == 0):
        local_training_time = time.time() - local_training_time
    if (rank == 0):
        local_testing_time = time.time()

    predict(rank=rank, model=model, device=device, test_data=test_data, test_target=test_target, do_log=do_log,
            comms=comms)

    if (rank == 0):
        local_testing_time = time.time() - local_testing_time
        print("Total Training Time : {}".format(local_training_time))
        print("Total Testing Time : {}".format(local_testing_time))
        save_log("stats.csv",
                 stat="{},{},{},{}".format(size, local_training_time, total_communication_time, local_testing_time))


def predict(rank, model, device, test_data=None, test_target=None, do_log=False, comms=None):
    """
    testing the trained model
    :rtype: None return
    """
    model.eval()
    test_loss = 0
    correct = 0
    total_samples = 0
    val1 = 0
    val2 = 0
    count = 0
    with torch.no_grad():
        for data, target in zip(test_data, test_target):
            # total_samples = total_samples + 1
            count = count + 1
            val1 = len(data)
            val2 = len(test_data)
            total_samples = (val1 * val2)
            data, target = data.to(device), target.to(device)
            data = np.reshape(data, (data.shape[0], 1, data.shape[1], data.shape[2])) / 128.0
            output = model(data)
            test_loss += F.nll_loss(output, target.long(), reduction='sum').item()  # sum up batch loss
            pred = output.argmax(dim=1, keepdim=True)  # get the index of the max log-probability
            correct += pred.eq(target.view_as(pred)).sum().item()
            if (rank == 0 and do_log):
                print(rank, count, len(data), len(test_data), data.shape, output.shape, correct, total_samples)

    test_loss /= (total_samples)
    local_accuracy = 100.0 * correct / total_samples
    global_accuracy = average_accuracy_mpi(torch.tensor(local_accuracy), comm=comms, world_size=4)
    if (rank == 0):
        print('\nTest set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)\n'.format(
            test_loss, correct, total_samples,
            global_accuracy.numpy()))


def train(world_rank=0, world_size=4, train_data=None, train_target=None, do_log=False, comms=None):
    """
    training the MNIST model
    :param int world_rank: current processor rank (MPI rank)
    :param int world_size: number of processes (MPI world size)
    :param tensor train_data: training data as pytorch tensor
    :param tensor train_target: training target as pytorch tensor
    :param boolean do_log: set logging
    :return:
    """
    torch.manual_seed(1234)
    model = Net()
    optimizer = optim.SGD(model.parameters(),
                          lr=0.01, momentum=0.5)

    num_batches = train_data.shape[1]

    if (world_rank == 0 and do_log):
        print("Started Training")
    total_data = len(train_data)
    epochs = 1
    total_steps = epochs * total_data
    local_time_communication = 0
    local_total_time_communication = 0

    for epoch in range(epochs):
        epoch_loss = 0.0
        count = 0
        for data, target in zip(train_data, train_target):
            data = np.reshape(data, (data.shape[0], 1, data.shape[1], data.shape[2])) / 128.0
            count = count + 1
            result = '{0:.4g}'.format((count / float(total_steps)) * 100.0)
            if (world_rank == 0):
                print("Progress {}% \r".format(result), end='\r')
            optimizer.zero_grad()
            output = model(data)
            # this comes with data loading mechanism use target or target.long()
            # depending on network specifications.
            target = target.long()
            loss = F.nll_loss(output, target)
            epoch_loss += loss.item()
            # print(epoch_loss)
            loss.backward()
            if (world_rank == 0):
                local_time_communication = time.time()
            average_gradients_mpi(model, comm=comms, world_size=4)
            if (world_rank == 0):
                local_time_communication = time.time() - local_time_communication
                local_total_time_communication = local_total_time_communication + local_time_communication
            optimizer.step()
        if (world_rank == 0):
            print('Rank ', world_rank, ', epoch ',
                  epoch, ': ', epoch_loss / num_batches)
    return model, local_total_time_communication


def format_mnist_data(data=None):
    """
    This method re-shapes the data to fit into the Network Input Shape
    :rtype: data re-formatted to fit to the network designed
    """
    data_shape = data.shape
    img_size = int(sqrt(data_shape[2]))
    data = np.reshape(data, (data_shape[0], data_shape[1], img_size, img_size))
    return data


def format_mnist_target(data=None):
    """
    Reshaping the mnist target values to fit into model
    :param data:
    :return:
    """
    data_shape = data.shape
    data = np.reshape(data, (data_shape[0], data_shape[1]))
    return data


def format_data(input_data=None, world_size=4, init_batch_size=128):
    """
    Specific For MNIST and 3 dimensional data
    This function is generated for MNIST only cannot be used in general for all data shapes
    :param input_data: data in numpy format with (N,M) format Here N number of samples
            M is the tensor length
    :return: For numpy we reshape this and return a tensor of the shape, (N, sqrt(M), sqrt(M))
    """
    bsz = int(init_batch_size / float(world_size))
    data = DataUtil.generate_minibatches(data=input_data, minibatch_size=bsz)
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


# world_size = int(os.environ['OMPI_COMM_WORLD_SIZE'])
# world_rank = int(os.environ['OMPI_COMM_WORLD_RANK'])
#
# train_data_save_path = "/tmp/parquet/train/"
# test_data_save_path = "/tmp/parquet/test/"
#
# train_data_file = str(world_rank) + ".data"
# test_data_file = str(world_rank) + ".data"
# train_target_file = str(world_rank) + ".target"
# test_target_file = str(world_rank) + ".target"

__BACKEND = 'mpi'

# LOAD DATA FROM DISK

## load train data
# train_data = read_from_disk(source_file=train_data_file, source_path=train_data_save_path)
train_data = format_data(input_data=train_data, world_size=world_size, init_batch_size=128)
train_data = format_mnist_data(data=train_data)
## load test data
# test_data = read_from_disk(source_file=test_data_file, source_path=test_data_save_path)
test_data = format_data(input_data=test_data, world_size=world_size, init_batch_size=16)
test_data = format_mnist_data(data=test_data)
## load train target
# train_target = read_from_disk(source_file=train_target_file, source_path=train_data_save_path)
train_target = format_data(input_data=train_target, world_size=world_size, init_batch_size=128)
train_target = format_mnist_target(data=train_target)
## load test target
# test_target = read_from_disk(source_file=test_target_file, source_path=test_data_save_path)
test_target = format_data(input_data=test_target, world_size=world_size, init_batch_size=16)
test_target = format_mnist_target(data=test_target)

if world_rank == 0:
    print("From File: ", train_data.shape, train_target.shape, test_data.shape, test_target.shape)

# print(train_data.shape, train_data[0][0][0:5][0:5])


do_log = False

# print("worker {} , data {} ".format(world_rank, train_target[0]))

# initialize training
launch(rank=world_rank, size=world_size, fn=train, backend=__BACKEND,
       train_data=train_data, train_target=train_target,
       test_data=test_data, test_target=test_target,
       do_log=do_log, comms=comm)
