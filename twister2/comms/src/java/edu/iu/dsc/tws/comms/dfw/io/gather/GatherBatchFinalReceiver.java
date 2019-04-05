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
package edu.iu.dsc.tws.comms.dfw.io.gather;

import java.util.List;
import java.util.Map;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;

public class GatherBatchFinalReceiver extends BaseGatherBatchReceiver {
  /**
   * Final receiver accepts a bulk receiver
   */
  private BulkReceiver bulkReceiver;

  /**
   * Constructs the gather batch final receiver
   * @param bulkReceiver the receiver
   */
  public GatherBatchFinalReceiver(BulkReceiver bulkReceiver) {
    this.bulkReceiver = bulkReceiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    bulkReceiver.init(cfg, expectedIds.keySet());
  }

  @Override
  protected boolean handleMessage(int task, Object message, int flags, int dest) {
    if (message instanceof List) {
      return bulkReceiver.receive(task, ((List<Object>) message).iterator());
    }
    return false;
  }

  @Override
  protected boolean sendSyncForward(boolean needsFurtherProgress, int target) {
    return !bulkReceiver.sync(target, 0);
  }

  @Override
  protected boolean isFilledToSend(int target, boolean sync) {
    if (!sync) {
      return false;
    }

    if (!allQueuesEmpty(messages.get(target))) {
      return false;
    }

    return gatheredValuesMap.get(target) != null && gatheredValuesMap.get(target).size() > 0;
  }
}
