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
package edu.iu.dsc.tws.data.api.assigner;

import edu.iu.dsc.tws.data.fs.io.InputSplit;
import edu.iu.dsc.tws.data.fs.io.InputSplitAssigner;

public class OrderedInputSplitAssigner implements InputSplitAssigner {
  private InputSplit[] splits;

  public OrderedInputSplitAssigner(InputSplit[] partitions) {
    splits = partitions;
  }

  @Override
  public InputSplit getNextInputSplit(String host, int taskId) {
    if (taskId < 0 || taskId > splits.length - 1) {
      throw new RuntimeException(String.format("We don't have a split for %d we only support "
          + "task ids in the range %d - %d", taskId, 0, splits.length));
    }
    return splits[taskId];
  }
}
