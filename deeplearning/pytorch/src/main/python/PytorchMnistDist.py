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

train_data_save_path = "/tmp/parquet/train/0"
test_data_save_path = "/tmp/parquet/test/"

# LOAD DATA FROM PARQUET
train_dataframe = ArrowUtils.read_from_table(train_data_save_path)

# CONVERT TO NUMPY
train_data = train_dataframe.to_numpy()
print(train_data.shape)
train_data_shape = train_data.shape
train_data = np.reshape(train_data, (train_data_shape[0], int(sqrt(train_data_shape[1])), int(sqrt(train_data_shape[1]))))
print(train_data.shape)



