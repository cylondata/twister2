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
package edu.iu.dsc.tws.comms.dfw.io.bcast;

import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.dfw.io.ReceiverState;
import edu.iu.dsc.tws.comms.dfw.io.TargetFinalReceiver;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class BcastBatchFinalReceiver extends TargetFinalReceiver {
  // the receiver
  private SingularReceiver receiver;

  /**
   * Keep the list of tuples for each target
   */
  private Int2ObjectOpenHashMap<Queue<Object>> readyToSend = new Int2ObjectOpenHashMap<>();

  public BcastBatchFinalReceiver(SingularReceiver receiver) {
    this.receiver = receiver;
  }

  public void init(Config cfg, DataFlowOperation operation,
                   Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, operation, expectedIds);
    this.receiver.init(cfg, expectedIds.keySet());
  }

  /**
   * Swap the messages to the ready queue
   *
   * @param dest the target
   * @param dests message queue to switch to ready
   */
  protected void merge(int dest, List<Object> dests) {
    if (!readyToSend.containsKey(dest)) {
      readyToSend.put(dest, new LinkedBlockingQueue<>(dests));
    } else {
      Queue<Object> ready = readyToSend.get(dest);
      ready.addAll(dests);
    }
    dests.clear();
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
  protected void addMessage(int target, List<Object> msgQueue, Object value) {
    msgQueue.add(value);
  }

  @Override
  protected boolean isAllEmpty(int target) {
    if (readyToSend.containsKey(target)) {
      Queue<Object> queue = readyToSend.get(target);
      if (queue.size() > 0) {
        return false;
      }
    }
    return true;
  }

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
      if (receiver.receive(target, val)) {
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
    return targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED
        && messages.get(target).isEmpty()
        && (readyToSend.get(target) == null
        || readyToSend.get(target) != null && !readyToSend.get(target).isEmpty());
  }

  @Override
  public void onSyncEvent(int target, byte[] value) {
    receiver.sync(target, value);
  }
}
