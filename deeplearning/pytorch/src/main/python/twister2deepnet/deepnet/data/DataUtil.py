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
from math import ceil
from math import sqrt
import numpy as np
import torch

class DataUtil:

    @staticmethod
    def generate_minibatches(data=None, minibatch_size=1):
        """
        TODO: Write proper method definition
        This method executes the following
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
        :param data: input data
        :param minibatch_size: mini-batch size expected in training
        :return: mini-batched data set
        """
        data_shape = data.shape
        num_batches = ceil(data_shape[0] / float(minibatch_size))
        remainder = data_shape[0] % minibatch_size
        is_remainder = False
        init_additional_idx = np.random.randint(10, size=5)
        additional_records = data[init_additional_idx,:]
        records = None
        if remainder > 0:
            is_remainder = True
            additional_idx = np.random.randint(data_shape[0], size=minibatch_size)
            record_idx = np.random.randint(data_shape[0], size=(num_batches - 1) * minibatch_size)
            additional_records = data[additional_idx, :]
            additional_records_shape = additional_records.shape
            additional_records = np.reshape(additional_records, (1, additional_records_shape[0],
                                                                 additional_records_shape[1]))
            records = data[record_idx, :]
            records = np.reshape(records, (num_batches-1, minibatch_size, data_shape[1]))
            records = np.concatenate((records, additional_records), axis=0)
        else:
            records = np.reshape(data, (num_batches, minibatch_size, data_shape[1]))
        tensors = torch.from_numpy(records)
        return tensors
