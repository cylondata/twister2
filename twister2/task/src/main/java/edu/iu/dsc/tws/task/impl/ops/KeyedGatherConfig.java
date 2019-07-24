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
package edu.iu.dsc.tws.task.impl.ops;

import java.util.Comparator;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.OperationNames;
import edu.iu.dsc.tws.api.task.graph.Edge;
import edu.iu.dsc.tws.task.impl.ComputeConnection;

public class KeyedGatherConfig extends AbstractKeyedOpsConfig<KeyedGatherConfig> {

  private boolean useDsk;
  private Comparator keyCompartor;
  private boolean grpByKey = true;
  private boolean srtByKey;

  public KeyedGatherConfig(String parent,
                           ComputeConnection computeConnection) {
    super(parent, OperationNames.KEYED_GATHER, computeConnection);
  }

  public KeyedGatherConfig useDisk(boolean useDisk) {
    this.useDsk = useDisk;
    return this.withProperty(CommunicationContext.USE_DISK, useDisk);
  }

  public <T> KeyedGatherConfig sortBatchByKey(boolean sortByKey,
                                              Comparator<T> keyComparator) {
    this.srtByKey = sortByKey;
    this.keyCompartor = keyComparator;
    return this.withProperty(CommunicationContext.SORT_BY_KEY, sortByKey)
        .withProperty(CommunicationContext.KEY_COMPARATOR, keyComparator);
  }

  public <T> KeyedGatherConfig sortBatchByKey(boolean sortByKey, Class<T> tClass,
                                              Comparator<T> keyComparator) {
    this.srtByKey = sortByKey;
    this.keyCompartor = keyComparator;
    return this.withProperty(CommunicationContext.SORT_BY_KEY, sortByKey)
        .withProperty(CommunicationContext.KEY_COMPARATOR, keyComparator);
  }

  public KeyedGatherConfig groupBatchByKey(boolean groupByKey) {
    this.grpByKey = groupByKey;
    return this.withProperty(CommunicationContext.GROUP_BY_KEY, groupByKey);
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
    if (this.keyCompartor != null) {
      newEdge.addProperty(CommunicationContext.KEY_COMPARATOR, this.keyCompartor);
    }

    newEdge.addProperty(CommunicationContext.SORT_BY_KEY, this.srtByKey);
    newEdge.addProperty(CommunicationContext.GROUP_BY_KEY, this.grpByKey);

    return newEdge;
  }
}
