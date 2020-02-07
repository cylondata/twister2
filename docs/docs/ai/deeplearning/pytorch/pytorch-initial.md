---
id: pytorchinitial
title: Pytorch Integration
sidebar_label: Pytorch Integration
---

In the initial Pytorch integration we use Twister2Flow with Apache Arrow, Pandas and Apache Parquet 
to link the data downloading, data pre-processing and deep learning workload. This is an 
experimental version of Twister2-Pytorch integration. In the experimental version we loads the data
with Arrow-Parquet format with the support of Pandas. And we use a intermediate step in connecting
different steps of the workflow. For instance the data downloading and data pre-processing steps are
linked with Twister2Flow but a file pointer is used to show the system where to write and read from 
the disk. In the training processes the program will look up to these file pointers to load the data.


## Data Downloading

For data downloading, Twister2:DeepNet API provides higher level APIs to load most prominent datasets. 
Currently we only support MNIST dataset and we are working on providing this support to other datasets. 
Twister2Flow can schedule this as the initial task of the AI workflow. 

## Data Pre-Processing

Twister2 with TSet interface on Python supports do the data pre-processing and data formatting before
loading to the training programme. Our APIs support all the state of the art collective communications.
With Pythonic TSet API you can use batch processing to process your data before passing to your deeplearning
code. 

## Training 

You can write the usual code that you write in Pytorch to train your algoirthm. In the current version, 
we support this with Apache Parquet, Pandas and Apache Arrow to load the data to the training programme. 
We are actively working on providing these capabilities with a in-memory data pipeline with Twister2. 
This capability will be added to Twister2Flow to provide a higher level API for the users. 

## Testing

Twister2 APIs has the ability to provide both batch and streaming inference. In an upcoming release
we will be providing TSet API addons to support inference at scale with batch and streaming mode. 


## MNIST Example 

### Prerequisites

1. Install Twister2 (Refer Getting Started Guide)
2. Install Pytorch from the source (To support distributed training, this is a must from Pytorch End)

### Installing

First, install Twister2 Python API

```bash
pip3 install twister2
```

Then install the experimental version of Twister2:DeepNet.

```bash
python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps twister2-deepnet-test
```

Then install the experimental version of Twister2Flow

```bash
python3 -m pip install --index-url https://test.pypi.org/simple/ --no-deps twister2flow-test
```

### Downloading Data

We use Twister2:DeepNet APIs to download the data. Create a file MnistDownload.py and place the following
code. 

```python
import os

from twister2deepnet.deepnet.datasets.MNIST import MNIST

__data_dir = '/tmp/twister2deepnet/mnist'

mnist_train = MNIST(source_dir=os.path.join(__data_dir, 'train'), train=True, transform=None)
mnist_train.download()

mnist_test = MNIST(source_dir=os.path.join(__data_dir, 'test'), train=False, transform=None)
mnist_test.download()
```

### Pre-Processing Data

In this example we just show case how you can pre-process the data. For MNIST there is no heavy
data processing logic involved. Create a file, Twister2PytorchMnist.py and place the following code. 

```python
import os
# CORE PYTWISTER2 IMPORTS
from twister2 import TSetContext
from twister2.Twister2Environment import Twister2Environment
from twister2.tset.fn.SourceFunc import SourceFunc
# TWISTER2 DEEPNET IMPORTS
from twister2deepnet.deepnet.data.UtilPanda import UtilPanda
from twister2deepnet.deepnet.examples.MnistDistributed import MnistDistributed
from twister2deepnet.deepnet.io.ArrowUtils import ArrowUtils
from twister2deepnet.deepnet.io.FileUtils import FileUtils

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
    for index, data in enumerate(itr):
        print("TRAIN", index, DATA_SAVE_PATHS[0], TRAIN_DATA_FILES[index], type(data), data.shape)
        save_to_disk(dataset=data, save_path=DATA_SAVE_PATHS[0], save_file=TRAIN_DATA_FILES[index])


def read_test_tuples(itr, collector, ctx: TSetContext):
    for index, data in enumerate(itr):
        print("TEST", index, DATA_SAVE_PATHS[1], TEST_DATA_FILES[index], type(data), data.shape)
        save_to_disk(dataset=data, save_path=DATA_SAVE_PATHS[1], save_file=TEST_DATA_FILES[index])


source_train = env.create_source(DataSource(train=True), PARALLELISM)
source_train.compute(read_train_tuples).cache()

source_test = env.create_source(DataSource(train=False), PARALLELISM)
source_test.compute(read_test_tuples).cache()

if world_rank == 0:
    print("Data SAVED to DISK")

```

### Training and Testing

For this example we do both training and testing within the Pytorch Programme. This is an experimental
version. We will be moving the data mini-batching section to the Twister2 Task to save more time
on the Pytorch training process. Create a file, PytorchMnistDist.py and place the following code.

```python
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


def launch(rank, size, fn, backend='tcp',
           train_data=None, train_target=None,
           test_data=None, test_target=None,
           do_log=False):
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
    dist.init_process_group(backend, rank=rank, world_size=size)
    # Setting CUDA FOR TRAINING
    #use_cuda = torch.cuda.is_available()
    # device = torch.device("cuda" if use_cuda else "cpu")

    device = torch.device("cpu")

    total_communication_time = 0
    local_training_time = 0
    local_testing_time = 0
    if (rank == 0):
        local_training_time = time.time()

    model, total_communication_time = fn(world_rank=rank, world_size=size, train_data=train_data,
                                         train_target=train_target, do_log=False)
    if (rank == 0):
        local_training_time = time.time() - local_training_time
    if (rank == 0):
        local_testing_time = time.time()

    predict(rank=rank, model=model, device=device, test_data=test_data, test_target=test_target, do_log=do_log)

    if (rank == 0):
        local_testing_time = time.time() - local_testing_time
        print("Total Training Time : {}".format(local_training_time))
        print("Total Testing Time : {}".format(local_testing_time))
        save_log("stats.csv",
                 stat="{},{},{},{}".format(size, local_training_time, total_communication_time, local_testing_time))


def predict(rank, model, device, test_data=None, test_target=None, do_log=False):
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
            test_loss += F.nll_loss(output, target, reduction='sum').item()  # sum up batch loss
            pred = output.argmax(dim=1, keepdim=True)  # get the index of the max log-probability
            correct += pred.eq(target.view_as(pred)).sum().item()
            if (rank == 0 and do_log):
                print(rank, count, len(data), len(test_data), data.shape, output.shape, correct, total_samples)

    test_loss /= (total_samples)
    local_accuracy = 100.0 * correct / total_samples
    global_accuracy = average_accuracy(torch.tensor(local_accuracy))
    if (rank == 0):
        print('\nTest set: Average loss: {:.4f}, Accuracy: {}/{} ({:.0f}%)\n'.format(
            test_loss, correct, total_samples,
            global_accuracy.numpy()))


def train(world_rank=0, world_size=4, train_data=None, train_target=None, do_log=False):
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
            average_gradients(model)
            if (world_rank == 0):
                local_time_communication = time.time() - local_time_communication
                local_total_time_communication = local_total_time_communication + local_time_communication
            optimizer.step()
        if (world_rank == 0):
            print('Rank ', dist.get_rank(), ', epoch ',
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


world_size = int(os.environ['OMPI_COMM_WORLD_SIZE'])
world_rank = int(os.environ['OMPI_COMM_WORLD_RANK'])

train_data_save_path = "/tmp/parquet/train/"
test_data_save_path = "/tmp/parquet/test/"

train_data_file = str(world_rank) + ".data"
test_data_file = str(world_rank) + ".data"
train_target_file = str(world_rank) + ".target"
test_target_file = str(world_rank) + ".target"

__BACKEND = 'mpi'


# LOAD DATA FROM DISK

## load train data
train_data = read_from_disk(source_file=train_data_file, source_path=train_data_save_path)
train_data = format_data(input_data=train_data, world_size=world_size, init_batch_size=128)
train_data = format_mnist_data(data=train_data)
## load test data
test_data = read_from_disk(source_file=test_data_file, source_path=test_data_save_path)
test_data = format_data(input_data=test_data, world_size=world_size, init_batch_size=16)
test_data = format_mnist_data(data=test_data)
## load train target
train_target = read_from_disk(source_file=train_target_file, source_path=train_data_save_path)
train_target = format_data(input_data=train_target, world_size=world_size, init_batch_size=128)
train_target = format_mnist_target(data=train_target)
## load test target
test_target = read_from_disk(source_file=test_target_file, source_path=test_data_save_path)
test_target = format_data(input_data=test_target, world_size=world_size, init_batch_size=16)
test_target = format_mnist_target(data=test_target)

do_log = False

# initialize training
launch(rank=world_rank, size=world_size, fn=train, backend=__BACKEND,
       train_data=train_data, train_target=train_target,
       test_data=test_data, test_target=test_target,
       do_log=do_log)

```

### Connecting Dots

In order to connect data downloading, data pre-processing, testing and training, we use Twister2Flow. 
Using Twister2Flow you can define the task graph as follows and schedule jobs. Twister2Flow initial
version is an initial experimental version. Create a file, Twister2Flow.py and place the following code. 

```python
from twister2flow.twister2.pipeline import PipelineGraph
from twister2flow.twister2.task.Twister2Task import Twister2Task
from twister2flow.twister2.task.PytorchTask import PytorchTask
from twister2flow.twister2.task.PythonTask import PythonTask

plg = PipelineGraph.PipelineGraph(name="UNNAMED_TASK")

download_task = PythonTask(name="download_task")
download_task.set_command("python3")
download_task.set_script_path(script_path="MnistDownload.py")
download_task.set_exec_path(exec_path=None)

twister2_task = Twister2Task(name="t2_task")
twister2_task.set_command("twister2 submit standalone python")
twister2_task.set_script_path(script_path="Twister2PytorchMnist.py")
twister2_task.set_exec_path(exec_path=None)

pytorch_task = PytorchTask(name="pytorch_task")
pytorch_task.set_command()
pytorch_task.set_script_path(script_path="PytorchMnistDist.py")
pytorch_task.set_exec_path(exec_path=None)

plg.add_task(download_task)
plg.add_task(twister2_task)
plg.add_task(pytorch_task)


print(str(plg))

plg.execute()

```

Then run the example

```bash
python3 Twister2Flow.py
```