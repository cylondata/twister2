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

from twister2deepnet.deepnet.data.DataLoader import DataLoader
from twister2deepnet.deepnet.exception.internal import ParameterError


class MnistDistributed:

    def __init__(self, source_dir=None, parallelism=4, world_rank=0):
        """
        :param parallelism: total process parallelism in data loading
        :param world_rank: current process id or MPI RANK
        """
        if source_dir is None:
            raise ParameterError("Source directory must be specified")

        self.source_dir = source_dir
        self.parallelism = parallelism
        self.rank = world_rank

    def load_train_data(self):
        """
        Here we assume the training data has features and labels
        TODO: Generalize this for unsupervised learning
        :return:
                train_x: features of the training data
                train_y: label of training data
                batch_size: number of elements per batch
        """
        dl = DataLoader(dataset="mnist", source_dir=self.source_dir, destination_dir=None,
                        transform=None)
        train_x, train_y, batch_size = dl.partition_numpy_dataset(self.parallelism, self.rank)
        return train_x, train_y, batch_size

    def load_test_data(self):
        """
        Here we assume the training data has features and labels
        TODO: Generalize this for unsupervised learning
        :return:
                train_x: features of the training data
                train_y: label of training data
                batch_size: number of elements per batch
        """
        dl = DataLoader(dataset="mnist", source_dir=self.source_dir, destination_dir=None,
                        transform=None)
        test_x, test_y, batch_size = dl.partition_numpy_dataset_test(self.parallelism, self.rank)
        return test_x, test_y, batch_size

#
# if __name__ == "__main__":
#     mniste = MnistDistributed()
#     mniste.load_data()
