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

from twister2deepnet.deepnet.datasets.Dataset import Dataset
from twister2deepnet.deepnet.io.FileUtils import FileUtils
from twister2deepnet.deepnet.data.Downloader import Downloader

class MNIST(Dataset):
    """
        'MNIST <http://yann.lecun.com/exdb/mnist/>`_ Dataset.
         'http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz',
         'http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz',
         'http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz',
         'http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz',
    """

    def __init__(self, dir=None, train=True, transform=None):
        self.dir = dir
        self.train = train
        self.transform = transform
        self.train_urls = [
            'http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz',
            'http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz'
        ]
        self.test_urls = [
            'http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz',
            'http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz'
        ]

    def download(self):
        FileUtils.mkdir_with_access(dir_path=self.dir)
        #
        # if self.train:
        #     # download train samples




