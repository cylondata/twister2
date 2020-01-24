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

""" Dataset partitioning helper """

import os
import numpy as np
import torch

from twister2deepnet.deepnet.data.DataPartitioner import DataPartitioner
from twister2deepnet.deepnet.datasets.MNIST import MNIST
from twister2deepnet.deepnet.exception.internal import ParameterError


class DataLoader:

    def __init__(self, dataset="mnist", source_dir=None, destination_dir=None,
                 transform=None):
        """

        """
        if source_dir is None:
            raise ParameterError("Source directory must be specified")
        else:
            self.source_dir = source_dir
        if destination_dir is None:
            self.destination_dir = self.source_dir
        else:
            self.destination_dir = destination_dir
        self.log = False
        self.dataset = dataset
        self.transform = transform

        self.train_save_files = None
        self.test_save_files = None

        self.__set_file_paths()

        # self.__TRAIN_DATA_FILE_PATH = "/home/vibhatha/github/PytorchExamples/datasets/train_data.npy"
        # self.__TRAIN_TARGET_FILE_PATH = "/home/vibhatha/github/PytorchExamples/datasets/train_target.npy"
        #
        # self.__TEST_DATA_FILE_PATH = "/home/vibhatha/github/PytorchExamples/datasets/test_data.npy"
        # self.__TEST_TARGET_FILE_PATH = "/home/vibhatha/github/PytorchExamples/datasets/test_target.npy"

    def partition_numpy_dataset(self, world_size=4, world_rank=0):
        """
        TODO: Re-write these logics in a dynamic way

        :rtype:
                train_set_data (training x parameters)
                train_set_target (training y parameters)
                bsz (batch size)
        """
        # print("Data Loading")
        dataset = np.load(self.__TRAIN_DATA_FILE_PATH)
        targets = np.load(self.__TRAIN_TARGET_FILE_PATH)
        bsz = int(128 / float(world_size))
        partition_sizes = [1.0 / world_size for _ in range(world_size)]
        # print("World Info {}/{}".format(world_rank, world_size))
        # print("Partition Sizes {}".format(partition_sizes))
        partition_data = DataPartitioner(dataset, partition_sizes)
        partition_data = partition_data.use(world_rank)
        train_set_data = torch.utils.data.DataLoader(partition_data,
                                                     batch_size=bsz,
                                                     shuffle=False)
        partition_target = DataPartitioner(targets, partition_sizes)
        partition_target = partition_target.use(world_rank)
        train_set_target = torch.utils.data.DataLoader(partition_target,
                                                       batch_size=bsz,
                                                       shuffle=False)
        return train_set_data, train_set_target, bsz

    def partition_numpy_dataset_test(self, world_size=4, world_rank=0):
        """
        TODO: Make this a dynamic implementation
        :param world_size:
        :return:
        """
        # print("Data Loading")

        dataset = np.load(self.__TEST_DATA_FILE_PATH)
        targets = np.load(self.__TEST_TARGET_FILE_PATH)
        # print("Data Size For Test {} {}".format(dataset.shape, targets.shape))

        bsz = int(16 / float(world_size))
        partition_sizes = [1.0 / world_size for _ in range(world_size)]
        # print("Partition Sizes {}".format(partition_sizes))
        print("World Info {}/{}".format(world_rank, world_size))
        print("Partition Sizes {}".format(partition_sizes))
        partition_data = DataPartitioner(dataset, partition_sizes)
        partition_data = partition_data.use(world_rank)
        train_set_data = torch.utils.data.DataLoader(partition_data,
                                                     batch_size=bsz,
                                                     shuffle=False)
        partition_target = DataPartitioner(targets, partition_sizes)
        partition_target = partition_target.use(world_rank)
        train_set_target = torch.utils.data.DataLoader(partition_target,
                                                       batch_size=bsz,
                                                       shuffle=False)
        return train_set_data, train_set_target, bsz

    def __set_file_paths(self):
        if self.dataset == 'mnist':
            self.train_save_files = MNIST.train_save_files()
            self.test_save_files = MNIST.test_save_files()
            self.__TRAIN_DATA_FILE_PATH = os.path.join(self.source_dir, "train",
                                                       self.train_save_files[0])
            self.__TRAIN_TARGET_FILE_PATH = os.path.join(self.source_dir, "train",
                                                         self.train_save_files[1])

            self.__TEST_DATA_FILE_PATH = os.path.join(self.source_dir, "test",
                                                      self.test_save_files[0])
            self.__TEST_TARGET_FILE_PATH = os.path.join(self.source_dir, "test",
                                                        self.test_save_files[1])


