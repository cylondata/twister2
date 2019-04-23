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
package edu.iu.dsc.tws.comms.dfw.io.partition;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;
import edu.iu.dsc.tws.comms.dfw.io.TargetPartialReceiver;

/**
 * This is the partial receiver for the partition operation.
 * Partial receiver is only going to get called for messages going to other destinations
 * We have partial receivers for each actual source, So even if the message is going to be forwarded
 * to a task within the same worker the message will still go through the partial receiver.
 */
public class PartitionPartialReceiver extends TargetPartialReceiver {

  private static final Logger LOG = Logger.getLogger(PartitionPartialReceiver.class.getName());

  private Set<Integer> sourcesWithSyncsSent = new HashSet<>();
  private int groupingSize = 100;

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    this.groupingSize = DataFlowContext.getNetworkPartitionBatchGroupingSize(cfg);
  }

  @Override
  protected void addSyncMessage(int source, int target) {
    super.addSyncMessage(source, target);
    this.sourcesWithSyncsSent.add(source);
  }

  @Override
  protected boolean isFilledToSend(int target) {
    return readyToSend.get(target) != null
        && readyToSend.get(target).size() > groupingSize
        && sourcesWithSyncsSent.size() == thisSources.size();
  }

  @Override
  public void clean() {
    super.clean();
    this.sourcesWithSyncsSent.clear();
  }
}
