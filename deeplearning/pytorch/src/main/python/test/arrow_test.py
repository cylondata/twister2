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
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import unittest

# Reference PyArrow Documentation: https://arrow.apache.org/docs/python/parquet.html

"""
ArrowTest class contains test cases for PyArrow with Parquet
Consider Test Case Order 
"""


class ArrowTest(unittest.TestCase):

    def init_env(self):
        directory = "/tmp/parquet/"
        parquet_table_path = directory + "table.parquet"
        if not os.path.isdir(directory):
            os.mkdir(directory, 0o777)
            print("Directory Created")
        else:
            print("Director Exists")

    def get_info(self):
        directory = "/tmp/parquet/"
        parquet_table_path = directory + "table.parquet"
        return directory, parquet_table_path

    def test_step_1_dir(self):
        self.init_env()
        directory, _ = self.get_info()
        self.assertIsNotNone(os.path.isdir(directory))

    def generate_data(self):
        df = pd.DataFrame({'one': [-1, np.nan, 2.5],
                           'two': ['foo', 'bar', 'baz'],
                           'three': [True, False, True]},
                          index=list('abc'))

        table = pa.Table.from_pandas(df)
        return table, df

    def test_step_2_generate_arrow_data(self):
        table, _ = self.generate_data()
        self.assertIsNotNone(table)

    def test_step_3_write_arrow_data(self):
        table, _ = self.generate_data()
        _, parquet_table_path = self.get_info()
        pq.write_table(table, parquet_table_path)

    def test_step_4_validate_table_creation(self):
        directory, parquet_table_path = self.get_info()
        self.assertTrue(os.path.exists(parquet_table_path), True)

    def test_step_6(self):
        print("Test Order")

    def test_step_5(self):
        _, parquet_table_path = self.get_info()
        table, df = self.generate_data()
        table2 = pq.read_table(parquet_table_path)
        self.assertEqual(table, table2)
        self.assertEqual(df.equals(table2.to_pandas()), True)

    def test_step_7_clear(self):
        _, parquet_table_path = self.get_info()
        self.assertTrue(os.path.exists(parquet_table_path), True)
        os.remove(parquet_table_path)


if __name__ == '__main__':
    unittest.main()
