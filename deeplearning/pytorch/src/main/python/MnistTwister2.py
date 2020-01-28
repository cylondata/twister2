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
