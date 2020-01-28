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

import numpy as np

from twister2deepnet.deepnet.exception.internal import ParameterError


class Normalize(object):

    def __init__(self, mean=1, std=1, data=None):
        """
        Normalize function only supports 2 dimensional data.
        For 3 Channel Images do not use this class.
        :param mean: mean
        :param std: standard deviation
        :param data: numpy array must be in shape of 3 (samples, W, H)
        """
        if not isinstance(data, np.ndarray):
            raise ParameterError("Data must be in numpy format")


        self.mean = mean
        self.std = std
        self.data = data

    def __call__(self, *args, **kwargs):
        normalized_data = self.data
        normalized_data = (self.data - self.mean) / self.std
        return normalized_data