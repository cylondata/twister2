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
package edu.iu.dsc.tws.comms.dfw.io.direct;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.dfw.io.AggregatedObjects;
import edu.iu.dsc.tws.comms.dfw.io.ReceiverState;
import edu.iu.dsc.tws.comms.dfw.io.TargetFinalReceiver;

public class DirectBatchFinalReceiver extends TargetFinalReceiver {
  private static final Logger LOG = Logger.getLogger(DirectBatchFinalReceiver.class.getName());

  /**
   * The receiver to be used to deliver the message
   */
  protected BulkReceiver receiver;

  /**
   * Keep the list of tuples for each target
   */
  private Map<Integer, List<Object>> readyToSend = new HashMap<>();

  public DirectBatchFinalReceiver(BulkReceiver receiver) {
    this.receiver = receiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    this.receiver.init(cfg, expectedIds.keySet());
  }

  @Override
  protected void addSyncMessage(int source, int target) {
    Set<Integer> sources = syncReceived.get(target);
    sources.add(source);
    targetStates.put(target, ReceiverState.ALL_SYNCS_RECEIVED);
  }

  @Override
  protected void addSyncMessageBarrier(int source, int target, byte[] barrier) {
    Set<Integer> sources = syncReceived.get(target);
    sources.add(source);
    targetStates.put(target, ReceiverState.ALL_SYNCS_RECEIVED);
    barriers.put(target, barrier);
  }

  @Override
  protected void merge(int dest, Queue<Object> dests) {
    if (!readyToSend.containsKey(dest)) {
      readyToSend.put(dest, new AggregatedObjects<>(dests));
    } else {
      List<Object> ready = readyToSend.get(dest);
      ready.addAll(dests);
    }
    dests.clear();
  }

  @Override
  protected boolean isAllEmpty() {
    boolean b = super.isAllEmpty();
    for (Map.Entry<Integer, List<Object>> e : readyToSend.entrySet()) {
      if (e.getValue().size() > 0) {
        return false;
      }
    }
    return b;
  }

  @Override
  protected boolean sendToTarget(int source, int target) {
    List<Object> values = readyToSend.get(target);

    if (values == null || values.isEmpty()) {
      return false;
    }

    if (receiver.receive(target, values.iterator())) {
      readyToSend.remove(target);
    } else {
      return false;
    }

    return true;
  }

  @Override
  protected boolean isFilledToSend(int target) {
    return targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED
        && messages.get(target).isEmpty();
  }

  @Override
  public void onSyncEvent(int target, byte[] value) {
    receiver.sync(target, value);
  }
}
