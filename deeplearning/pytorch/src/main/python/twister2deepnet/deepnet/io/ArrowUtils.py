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
import pyarrow as pa
import pyarrow.parquet as pq


class ArrowUtils:


    @staticmethod
    def create_to_table(dataFrame=None):
        table = None
        if dataFrame is None:
            pass
        else:
            table = pa.Table.from_pandas(dataFrame)

        return table

    @staticmethod
    def write_to_table(table, save_path=None):
        if not os.path.exists(save_path):
            pq.write_table(table, save_path)
        else:
            pass

    @staticmethod
    def read_from_table(path_to_table):
        table = None
        if not os.path.exists(path_to_table):
            raise Exception("Path Doesn't Exist")
        else:
            table = pq.read_table(path_to_table).to_pandas()
        return table