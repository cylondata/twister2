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
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

public class TargetPartialReceiver extends TargetReceiver {
  private static final Logger LOG = Logger.getLogger(TargetPartialReceiver.class.getName());
  /**
   * Keep state about the targets
   */
  protected Map<Integer, ReceiverState> sourceStates = new HashMap<>();

  /**
   * Keep track what are the targets we've sent syncs to
   */
  protected Map<Integer, Set<Integer>> syncSent = new HashMap<>();

  /**
   * Keep the list of tuples for each target
   */
  protected Map<Integer, List<Object>> readyToSend = new HashMap<>();

  /**
   * The barriers for each source
   */
  protected Map<Integer, byte[]> barriers = new HashMap<>();

  /**
   * State is cleared
   */
  protected boolean stateCleared = false;

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    thisSources = TaskPlanUtils.getTasksOfThisWorker(op.getTaskPlan(), op.getSources());
    thisDestinations = op.getTargets();
    for (int target : thisDestinations) {
      messages.put(target, new LinkedBlockingQueue<>());
    }
    // we are at the receiving state
    for (int source : thisSources) {
      sourceStates.put(source, ReceiverState.RECEIVING);
      syncSent.put(source, new HashSet<>());
    }
  }

  /**
   * Swap the messages to the ready queue
   * @param dest the target
   * @param dests message queue to switch to ready
   */
  protected void merge(int dest, Queue<Object> dests) {
    if (!readyToSend.containsKey(dest)) {
      readyToSend.put(dest, new AggregatedObjects<>(dests));
    } else {
      List<Object> ready = readyToSend.get(dest);
      ready.addAll(dests);
    }
    dests.clear();
  }

  /**
   * This gets called with a represented source
   * @param source the sources the represented source
   * @param target the target the true target
   * @return true if send if successful or nothing to send
   */
  @Override
  protected boolean sendToTarget(int source, int target) {
    List<Object> values = readyToSend.get(target);
    if (values != null && values.size() > 0) {
      if (operation.sendPartial(source, values, 0, target)) {
        readyToSend.remove(target);
      } else {
        return false;
      }
    }
    return true;
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
  protected boolean isFilledToSend(int target) {
    return readyToSend.get(target) != null && readyToSend.get(target).size() > 0;
  }

  @Override
  protected void addSyncMessage(int source, int target) {
    sourceStates.put(source, ReceiverState.ALL_SYNCS_RECEIVED);
  }

  @Override
  protected void addSyncMessageBarrier(int source, int target, byte[] barrier) {
    sourceStates.put(source, ReceiverState.ALL_SYNCS_RECEIVED);
    syncState = SyncState.BARRIER_SYNC;
    barriers.put(source, barrier);
  }

  @Override
  protected boolean canAcceptMessage(int source, int target) {
    if (sourceStates.get(source) == ReceiverState.ALL_SYNCS_RECEIVED
        || sourceStates.get(target) == ReceiverState.SYNCED) {
      return false;
    }

    Queue<Object> msgQueue = messages.get(target);
    return msgQueue.size() < highWaterMark;
  }

  @Override
  public void onFinish(int source) {
    addSyncMessage(source, 0);
  }

  @Override
  public boolean sync() {
    boolean allSynced = true;

    for (Map.Entry<Integer, ReceiverState> e : sourceStates.entrySet()) {
      int source = e.getKey();
      if (e.getValue() == ReceiverState.RECEIVING) {
        allSynced = false;
        continue;
      }

      // if we have synced no need to go forward
      if (e.getValue() == ReceiverState.SYNCED) {
        continue;
      }

      Set<Integer> finishedDestPerSource = syncSent.get(source);
      for (int dest : thisDestinations) {
        if (!finishedDestPerSource.contains(dest)) {

          byte[] message;
          int flags;
          if (syncState == SyncState.SYNC) {
            flags = MessageFlags.SYNC_EMPTY;
            message = new byte[1];
          } else {
            flags = MessageFlags.SYNC_BARRIER;
            message = barriers.get(source);
          }

          if (operation.sendPartial(source, message, flags, dest)) {
            finishedDestPerSource.add(dest);

            if (finishedDestPerSource.size() == thisDestinations.size()) {
              sourceStates.put(source, ReceiverState.SYNCED);
            }
          } else {
            allSynced = false;
            // no point in going further
            break;
          }
        }
      }
    }

    if (allSynced && !stateCleared) {
      for (int t : thisDestinations) {
        clearTarget(t);
      }

      for (Map.Entry<Integer, Set<Integer>> e : syncSent.entrySet()) {
        e.getValue().clear();
      }

      stateCleared = true;
    }

    return allSynced;
  }

  @Override
  public void clean() {
    for (int t : thisDestinations) {
      clearTarget(t);
    }

    for (int source : thisSources) {
      sourceStates.put(source, ReceiverState.RECEIVING);
    }
    barriers.clear();
    stateCleared = false;
  }
}
