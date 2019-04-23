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
package edu.iu.dsc.tws.comms.dfw.io.allgather;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.dfw.io.AggregatedObjects;
import edu.iu.dsc.tws.comms.dfw.io.ReceiverState;
import edu.iu.dsc.tws.comms.dfw.io.TargetFinalReceiver;

public class BcastGatheStreamingReceiver extends TargetFinalReceiver {
  /**
   * The receiver to be used to deliver the message
   */
  private BulkReceiver receiver;

  /**
   * Keep the list of tuples for each target
   */
  private Map<Integer, Queue<Object>> readyToSend = new HashMap<>();

  public BcastGatheStreamingReceiver(BulkReceiver receiver) {
    this.receiver = receiver;
  }

  public void init(Config cfg, DataFlowOperation operation,
                   Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, operation, expectedIds);
    this.receiver.init(cfg, expectedIds.keySet());
  }

  @Override
  protected void addSyncMessage(int source, int target) {
    targetStates.put(target, ReceiverState.ALL_SYNCS_RECEIVED);
  }

  @Override
  protected void addSyncMessageBarrier(int source, int target, byte[] barrier) {
    targetStates.put(target, ReceiverState.ALL_SYNCS_RECEIVED);
    barriers.put(target, barrier);
  }

  @Override
  protected void addMessage(Queue<Object> msgQueue, Object value) {
    msgQueue.add(value);
  }

  /**
   * Swap the messages to the ready queue
   * @param dest the target
   * @param dests message queue to switch to ready
   */
  protected void merge(int dest, Queue<Object> dests) {
    if (!readyToSend.containsKey(dest)) {
      readyToSend.put(dest, new LinkedBlockingQueue<>(dests));
    } else {
      Queue<Object> ready = readyToSend.get(dest);
      ready.addAll(dests);
    }
    dests.clear();
  }

  @Override
  protected boolean isAllEmpty() {
    boolean b = super.isAllEmpty();
    for (Map.Entry<Integer, Queue<Object>> e : readyToSend.entrySet()) {
      if (e.getValue().size() > 0) {
        return false;
      }
    }
    return b;
  }

  @SuppressWarnings("unchecked")
  @Override
  protected boolean sendToTarget(int source, int target) {
    Queue<Object> values = readyToSend.get(target);

    if (values == null || values.isEmpty()) {
      return false;
    }

    // if we send this list successfully
    Object val = values.peek();

    if (val == null) {
      return false;
    }

    while (val != null) {
      if (!(val instanceof AggregatedObjects)) {
        throw new RuntimeException("Failed to receive this type: " + val.getClass());
      }

      if (receiver.receive(target, ((AggregatedObjects<Object>) val).iterator())) {
        values.poll();
        val = values.peek();
      } else {
        return false;
      }
    }

    return true;
  }

  @Override
  protected boolean isFilledToSend(int target) {
    return readyToSend.get(target) != null && readyToSend.get(target).size() > 0;
  }

  @Override
  public void onSyncEvent(int target, byte[] value) {
    receiver.sync(target, value);
  }
}
