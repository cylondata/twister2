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


DATA_FOLDER = '/tmp/twister2deepnet/mnist/'

TRAIN_DATA_SAVE_PATH = "/tmp/parquet/train/"
TEST_DATA_SAVE_PATH = "/tmp/parquet/test/"

PARALLELISM = 4

env = Twister2Environment(resources=[{"cpu": 1, "ram": 512, "instances": PARALLELISM}])
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
    """
    calculating the average models in the distributed training
    :param model: averaged model over allreduce operation
    """
    size = float(dist.get_world_size())
    for param in model.parameters():
        dist.all_reduce(param.grad.data, op=dist.ReduceOp.SUM)
        param.grad.data /= size


def average_accuracy(local_accuracy):
    """
    calculating the average accuracy in the distributed testing
    :param local_accuracy: accuracy calculated in a processes
    :return: average accuracy all over all processes
    """
    size = float(dist.get_world_size())
    dist.all_reduce(local_accuracy, op=dist.ReduceOp.SUM)
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
        print(PARALLELISM, world_rank, DATA_FOLDER, TRAIN_DATA_SAVE_PATH, TEST_DATA_SAVE_PATH)
        self.load_data()

    def has_next(self):
        return self.i < len(self.data_load)

    def next(self):
        res = self.data_load[self.i]
        self.i = self.i + 1
        # TODO: packaging a message with meta
        #message = np.array([[self.i], [res]])
        return res

    def load_data(self):
        if not self.is_loaded:
            print("Data Loading {}".format(self.i))
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


def read_train_tuples(itr, collector, ctx: TSetContext):
    train_data = next(itr)
    train_target = next(itr)
    print(train_data.shape, train_target.shape)



def read_test_tuples(itr, collector, ctx: TSetContext):
    test_data = next(itr)
    test_target = next(itr)
    print(test_data.shape, test_target.shape)



source_train = env.create_source(DataSource(train=True), PARALLELISM)
source_train.compute(read_train_tuples).cache()

source_test = env.create_source(DataSource(train=False), PARALLELISM)
source_test.compute(read_test_tuples).cache()

if world_rank == 0:
    print("Data SAVED to DISK")



