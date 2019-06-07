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
package edu.iu.dsc.tws.api.task.ops;

import java.util.Comparator;

import edu.iu.dsc.tws.api.task.ComputeConnection;
import edu.iu.dsc.tws.executor.core.OperationNames;
import edu.iu.dsc.tws.task.graph.Edge;

public class KeyedGatherConfig extends AbstractKeyedOpsConfig<KeyedGatherConfig> {

  private boolean useDsk;
  private Comparator keyCompartor;
  private boolean grpByKey = true;
  private boolean srtByKey;

  protected KeyedGatherConfig(String parent,
                              ComputeConnection computeConnection) {
    super(parent, OperationNames.KEYED_GATHER, computeConnection);
  }

  public KeyedGatherConfig useDisk(boolean useDisk) {
    this.useDsk = useDisk;
    return this.withProperty("use-disk", useDisk);
  }

  public <T> KeyedGatherConfig sortBatchByKey(boolean sortByKey,
                                              Comparator<T> keyComparator) {
    this.srtByKey = sortByKey;
    this.keyCompartor = keyComparator;
    return this.withProperty("sort-by-key", sortByKey)
        .withProperty("key-comparator", keyComparator);
  }

  public <T> KeyedGatherConfig sortBatchByKey(boolean sortByKey, Class<T> tClass,
                                              Comparator<T> keyComparator) {
    this.srtByKey = sortByKey;
    this.keyCompartor = keyComparator;
    return this.withProperty("sort-by-key", sortByKey)
        .withProperty("key-comparator", keyComparator);
  }

  public KeyedGatherConfig groupBatchByKey(boolean groupByKey) {
    this.grpByKey = groupByKey;
    return this.withProperty("group-by-key", groupByKey);
  }

  @Override
  void validate() {
    if (this.srtByKey && this.keyCompartor == null) {
      failValidation("KeyedGather operation configured to sort by key. "
          + "But Key Comparator is not specified.");
    }
  }

  @Override
  protected Edge updateEdge(Edge newEdge) {
    return newEdge;
  }
}
