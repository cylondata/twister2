//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package edu.iu.dsc.tws.dataset;

import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.data.api.InputPartitioner;
import edu.iu.dsc.tws.data.fs.io.InputSplit;

public class DataSource<T> extends DataSet<T> {
  private static final Logger LOG = Logger.getLogger(DataSource.class.getName());

  private InputPartitioner<T, ?> input;

  private Map<Integer, InputSplit<T>> splits = new HashMap<>();

  public DataSource(Config config, InputPartitioner<T, ?> input, int numSplits) {
    super(0);

    this.input = input;
    this.input.configure(config);
    try {
      InputSplit<T>[] sps = this.input.createInputSplits(numSplits);
      for (InputSplit<T> split : sps) {
        splits.put(split.getSplitNumber(), split);
      }

      for (InputSplit<T> split : sps) {
        split.configure(config);
        // open the split
        split.open();
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to create the input splits");
    }
  }

  public InputSplit<T> getSplit(int id) {
    return splits.get(id);
  }
}
