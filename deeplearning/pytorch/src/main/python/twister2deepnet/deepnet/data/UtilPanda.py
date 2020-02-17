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

import pandas as pd
import numpy as np
from twister2deepnet.deepnet.exception.internal import ParameterError


class UtilPanda:

    @staticmethod
    def convert_numpy_to_pandas(ndarray=None):
        dataframe = None
        if not isinstance(ndarray, np.ndarray):
            raise ParameterError("Input is {}, but expected {}".format(type(ndarray),
                                                                       type(np.ndarray)))
        else:
            dataframe = pd.DataFrame(ndarray)

        return dataframe

    @staticmethod
    def convert_partition_to_pandas(partition=None):
        """
        Here we convert a Parition data type into a Pandas DataFrame
        The content in each segment of a partition is made flatten to support 2-D nature of a
        Pandas DataFrame
        Warning: At the end usage make sure to unwrap to fit the original shape
        :param partition: Input is the Partition Object of Data from Pytorch Data Source
        :return: Pandas dataframe
        """
        dataframe = None
        lst = []
        if not len(partition) > 0:
            pass

        for item in partition:
            ## Check 2D data
            if len(item.shape) > 1:
                lst.append(item.flatten())
            else:
                lst.append(item)

        lst_array = np.array(lst)
        print(lst_array.shape)
        dataframe = pd.DataFrame(lst_array)
        return dataframe
