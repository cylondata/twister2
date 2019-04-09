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
package edu.iu.dsc.tws.comms.dfw.io;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public abstract class TargetFinalReceiver extends TargetSyncReceiver {
  private static final Logger LOG = Logger.getLogger(TargetFinalReceiver.class.getName());
  /**
   * Keep weather we have received a sync from a source
   */
  private Map<Integer, Set<Integer>> syncReceived = new HashMap<>();

  /**
   * Keep state about the targets
   */
  private Map<Integer, ReceiverState> targetStates = new HashMap<>();

  /**
   * Keep the list of tuples for each target
   */
  protected Map<Integer, Queue<Object>> readyToSend = new HashMap<>();

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    thisSources = op.getSources();
    thisDestinations = TaskPlanUtils.getTasksOfThisWorker(op.getTaskPlan(), op.getTargets());

    for (Integer target : expectedIds.keySet()) {
      syncReceived.put(target, new HashSet<>());
      targetStates.put(target, ReceiverState.RECEIVING);
    }

    for (int target : thisDestinations) {
      messages.put(target, new LinkedBlockingQueue<>());
    }
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

  @Override
  protected void addSyncMessage(int source, int target) {
    Set<Integer> sources = syncReceived.get(target);
    sources.add(source);
    if (sources.equals(this.thisSources)) {
      targetStates.put(target, ReceiverState.ALL_SYNCS_RECEIVED);
    }
  }

  @Override
  protected boolean canAcceptMessage(int source, int target) {
    if (targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED
        || targetStates.get(target) == ReceiverState.SYNCED) {
      return false;
    }

    Queue<Object> msgQueue = messages.get(target);
    return msgQueue.size() < highWaterMark;
  }

  @Override
  protected boolean sync() {
    boolean allSynced = true;
    for (int target : thisDestinations) {
      if (targetStates.get(target) == ReceiverState.RECEIVING) {
        allSynced = false;
      }

      if (targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED) {
        onSyncEvent(target);
        targetStates.put(target, ReceiverState.SYNCED);
      }
    }
    return allSynced;
  }

  @Override
  protected boolean isFilledToSend(int target) {
    return readyToSend.get(target) != null && readyToSend.get(target).size() > 0;
  }
}
