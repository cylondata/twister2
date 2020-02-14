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

from twister2deepnet.deepnet.datasets.MNIST import MNIST
import numpy as np
from twister2deepnet.deepnet.transform.Normalize import Normalize

__data_dir = '/tmp/twister2deepnet/mnist'


mnist_train = MNIST(source_dir=os.path.join(__data_dir, 'train'), train=True, transform=None)
mnist_train.download()

mnist_test = MNIST(source_dir=os.path.join(__data_dir, 'test'), train=False, transform=None)
mnist_test.download()

images = mnist_train.images
labels = mnist_train.labels


images = images.astype(np.float32)

normalize = Normalize(mean=0, std=255.0, data=images)
new_images2 = normalize()