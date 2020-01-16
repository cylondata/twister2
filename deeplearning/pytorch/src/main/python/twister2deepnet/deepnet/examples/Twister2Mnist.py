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

from twister2deepnet.Twister2Environment import Twister2Environment
from twister2deepnet.tset.fn.SourceFunc import SourceFunc



class Twister2Mnist:

    def __init__(self):
        self.data_train_path = ""
        self.data_test_path = ""
        self.target_train_path = ""
        self.target_test_path = ""

