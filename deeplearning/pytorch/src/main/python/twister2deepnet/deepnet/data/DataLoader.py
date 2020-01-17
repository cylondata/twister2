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
import numpy as np
import torch

from twister2deepnet.deepnet.data.DataPartitioner import DataPartitioner


class DataLoader:

    def __init__(self):
        """
        TODO: Need to replace this with a remote data repository
        """
        self.__TRAIN_DATA_FILE_PATH = "/home/vibhatha/github/PytorchExamples/datasets/train_data.npy"
        self.__TRAIN_TARGET_FILE_PATH = "/home/vibhatha/github/PytorchExamples/datasets/train_target.npy"

    def partition_numpy_dataset(self, world_size=4, world_rank=0):
        """

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
