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

from twister2deepnet.deepnet.datasets.Dataset import Dataset
from twister2deepnet.deepnet.io.FileUtils import FileUtils
from twister2deepnet.deepnet.data.Downloader import Downloader
from twister2deepnet.deepnet.exception.internal import ParameterError

from mlxtend.data import loadlocal_mnist
import numpy as np


class MNIST(Dataset):
    """
          MNIST class is a util class which downloads the mnist dataset from the following urls.
         'MNIST <http://yann.lecun.com/exdb/mnist/>`_ Dataset.
         'http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz',
         'http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz',
         'http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz',
         'http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz',

         In reading the MNIST data we use the following tool
         It can be installed with 'pip install mlxtend' or 'pip3 install mlxtend '
         MNIST Reader can be found in the following Git Repository
         https://github.com/rasbt/mlxtend

         This class saves the data to the disk using Numpy *.npy format
         Use can extend from this class and write custom wrappers to support specific use cases
    """

    def __init__(self, source_dir=None, destination_dir=None, train=True, transform=None,
                 clean=False, save_numpy=True):
        """

        :param source_dir: directory to which we download the MNIST data
        :param destination_dir: directory to which we extract the MNIST data (can be source_dir)
        :param train: boolean flag to download training or testing data
        :param transform: transform the data to numpy or pytorch tensor (TODO)
        :param clean: boolean flag to clean up the downloaded archive files
        :param save_numpy: flag to save as *.npy files
        """
        self.source_dir = source_dir
        if destination_dir is None:
            self.destination_dir = self.source_dir
        else:
            self.destination_dir = destination_dir
        self.train = train
        self.transform = transform
        self.clean = clean
        self.save_numpy = save_numpy
        self.train_urls = [
            'http://yann.lecun.com/exdb/mnist/train-images-idx3-ubyte.gz',
            'http://yann.lecun.com/exdb/mnist/train-labels-idx1-ubyte.gz'
        ]
        self.test_urls = [
            'http://yann.lecun.com/exdb/mnist/t10k-images-idx3-ubyte.gz',
            'http://yann.lecun.com/exdb/mnist/t10k-labels-idx1-ubyte.gz'
        ]

        self.train_file_names = [
            'train-images-idx3-ubyte.gz',
            'train-labels-idx1-ubyte.gz'
        ]

        self.test_file_names = [
            't10k-images-idx3-ubyte.gz',
            't10k-labels-idx1-ubyte.gz'
        ]

    def __file_exist(self, path=None):
        exists = False
        if os.path.exists(path):
            exists = True
        return exists

    def download_by_type(self, urls, file_names):
        """
        Downloads the data by urls and filenames
        TODO: replace the file_names with URL basename
        :param urls: urls from which data is downloaded
        :param file_names: corresponding files (TODO: replace this with basename)
        """
        for url, file_name in zip(urls, file_names):
            if not self.__file_exist(path=os.path.join(self.source_dir, file_name)):
                Downloader.download(url=url, save_path=self.source_dir, file_name=file_name)
                _full_file_name = os.path.join(self.destination_dir, file_name)
                FileUtils.extract_archive(_full_file_name)

    def save_downloads(self, urls=None):
        """
        save the downloaded files to numpy format
        :param urls: urls from which data is being downloaded
        """
        image_file_name = os.path.basename(urls[0])
        image_file_path = os.path.join(self.destination_dir, image_file_name.split(".")[0])

        label_file_name = os.path.basename(urls[1])
        label_file_path = os.path.join(self.destination_dir, label_file_name.split(".")[0])

        if self.__file_exist(path=image_file_path) and self.__file_exist(path=label_file_path):
            print("Files Exist {}, {}".format(image_file_path, label_file_path))
            self.save_as_numpy(image_path=image_file_path, label_path=label_file_path,
                               image_save_path=image_file_path+".npy",
                               label_save_path=label_file_path+".npy")
        else:
            raise ParameterError("File cannot be located {}".format(image_file_path))
        # _full_file_save_name = os.path.join(self.destination_dir, file_name + ".npy")
        # if self.save_numpy:
        #     self.save_as_numpy(image_path=)

    def download(self):
        """
        Downloads the data and save to the disk
        """
        FileUtils.mkdir_branch_with_access(dir_path=self.source_dir)

        if self.train:
            # download train samples
            self.download_by_type(urls=self.train_urls, file_names=self.train_file_names)
            self.save_downloads(urls=self.train_urls)
        else:
            # download test samples
            self.download_by_type(urls=self.test_urls, file_names=self.test_file_names)
            self.save_downloads(urls=self.test_urls)

    def save_as_numpy(self, image_path=None, label_path=None,
                      image_save_path=None, label_save_path=None):
        """
        save the files to *.npy format
        :param image_path: MNIST image data path (extract file path)
        :param label_path: MNIST image label path (extract file path)
        :param image_save_path: MNIST image save path as npy
        :param label_save_path: MNIST label save path as npy
        """
        images, labels = loadlocal_mnist(images_path=image_path, labels_path=label_path)
        if not FileUtils.check_exist_with_message(file_path=image_save_path, message="Images Already Saved!"):
            np.save(image_save_path, images)
        if not FileUtils.check_exist_with_message(file_path=label_save_path, message="Labels Already Saved!"):
            np.save(label_save_path, labels)



