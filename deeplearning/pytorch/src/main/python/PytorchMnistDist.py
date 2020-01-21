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
# NUMPY IMPORTS
import numpy as np
import pandas as pd
from math import ceil
from math import sqrt
from twister2deepnet.deepnet.examples.MnistDistributed import MnistDistributed
from twister2deepnet.deepnet.network.MnistNet import  MnistNet
from twister2deepnet.deepnet.data.UtilPanda import UtilPanda
from twister2deepnet.deepnet.io.FileUtils import FileUtils
from twister2deepnet.deepnet.io.ArrowUtils import ArrowUtils

import torch.nn as nn
import torch
import torch.optim as optim
import torch.nn.functional as F

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


def format_data(input_data=None):
    """
    Specific For MNIST and 3 dimensional data
    This function is generated for MNIST only cannot be used in general for all data shapes
    :param input_data: data in numpy format with (N,M) format Here N number of samples
            M is the tensor length
    :return: For numpy we reshape this and return a tensor of the shape, (N, sqrt(M), sqrt(M))
    """
    data_shape = input_data.shape
    data = np.reshape(input_data, (data_shape[0], int(sqrt(data_shape[1])), int(sqrt(data_shape[1]))))
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
train_data = format_data(input_data=train_data)
## load test data
test_data = read_from_disk(source_file=test_data_file, source_path=test_data_save_path)
test_data = format_data(input_data=test_data)
## load train target
train_target = read_from_disk(source_file=train_target_file, source_path=train_data_save_path)
train_target = format_data(input_data=train_target)
## load test target
test_target = read_from_disk(source_file=test_target_file, source_path=test_data_save_path)
test_target = format_data(input_data=test_target)



# train_data_shape = train_data.shape
# train_data = np.reshape(train_data, (train_data_shape[0], int(sqrt(train_data_shape[1])), int(sqrt(train_data_shape[1]))))

print(train_data.shape, train_target.shape, test_data.shape, test_target.shape)



