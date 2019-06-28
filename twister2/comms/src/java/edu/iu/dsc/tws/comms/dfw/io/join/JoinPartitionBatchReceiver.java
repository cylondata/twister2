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
package edu.iu.dsc.tws.comms.dfw.io.join;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.SingularReceiver;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.dfw.io.AggregatedObjects;
import edu.iu.dsc.tws.comms.dfw.io.ReceiverState;
import edu.iu.dsc.tws.comms.dfw.io.TargetFinalReceiver;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class JoinPartitionBatchReceiver extends TargetFinalReceiver {
  private static final Logger LOG = Logger.getLogger(JoinPartitionBatchReceiver.class.getName());
  /**
   * The receiver to be used to deliver the message
   */
  protected SingularReceiver receiver;

  /**
   * Keep the list of tuples for each target
   */
  protected Int2ObjectOpenHashMap<List<Object>> readyToSend = new Int2ObjectOpenHashMap<>();

  private int tag;

  public JoinPartitionBatchReceiver(SingularReceiver receiver, int tag) {
    this.receiver = receiver;
    this.tag = tag;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    receiver.init(cfg, expectedIds.keySet());
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
  protected boolean sendToTarget(int source, int target) {
    List<Object> values = readyToSend.get(target);

    calledReceive = true;
    if (values == null || values.isEmpty()) {
      values = new ArrayList<>();
    }

    if (receiver.receive(target, values)) {
      readyToSend.remove(target);
    } else {
      return false;
    }

    return true;
  }

  @Override
  protected boolean isAllEmpty(int target) {
    if (readyToSend.containsKey(target)) {
      List<Object> queue = readyToSend.get(target);
      return queue.size() <= 0;
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
