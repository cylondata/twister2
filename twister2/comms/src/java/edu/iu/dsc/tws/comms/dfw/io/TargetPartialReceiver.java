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
import java.util.TreeSet;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.messaging.MessageFlags;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.utils.TaskPlanUtils;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public class TargetPartialReceiver extends TargetReceiver {
  private static final Logger LOG = Logger.getLogger(TargetPartialReceiver.class.getName());
  /**
   * Keep state
   */
  protected Map<Integer, ReceiverState> sourceStates = new HashMap<>();

  /**
   * Keep track what are the targets we've sent syncs to
   */
  protected Map<Integer, Set<Integer>> syncSent = new HashMap<>();

  /**
   * Keep the list of tuples for each target
   */
  protected Int2ObjectOpenHashMap<List<Object>> readyToSend = new Int2ObjectOpenHashMap<>();

  /**
   * The barriers for each source
   */
  protected Map<Integer, byte[]> barriers = new HashMap<>();

  /**
   * State is cleared
   */
  protected boolean stateCleared = false;

  /**
   * Source array for iterations
   */
  private int[] thisSourceArray;

  /**
   * This source array for iteration
   */
  private int[] sourceArray;

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    thisSources = TaskPlanUtils.getTasksOfThisWorker(op.getLogicalPlan(), op.getSources());
    int index = 0;
    thisSourceArray = new int[thisSources.size()];
    for (int s : thisSources) {
      this.thisSourceArray[index++] = s;
    }

    Set<Integer> thisWorkerTargets = TaskPlanUtils.getTasksOfThisWorker(op.getLogicalPlan(),
        op.getTargets());
    // we are going to send the sync to worker target at last, this will ensure that we are not
    // going to start sorting before we send the syncs to other nodes
    thisDestinations = new HashSet<>(new TreeSet<>((o1, o2) -> {
      if (thisWorkerTargets.contains(o1) && thisWorkerTargets.contains(o2)) {
        return o1 - o2;
      } else if (thisWorkerTargets.contains(o1)) {
        return 1;
      } else if (thisWorkerTargets.contains(o2)) {
        return -1;
      } else {
        return o1 - o2;
      }
    }));
    thisDestinations.addAll(op.getTargets());

    this.targets = new int[thisDestinations.size()];
    index = 0;
    for (int t : thisDestinations) {
      targets[index++] = t;
    }

    for (int target : thisDestinations) {
      messages.put(target, new LinkedBlockingQueue<>());
    }

    index = 0;
    sourceArray = new int[thisSources.size()];
    // we are at the receiving state
    for (int source : thisSources) {
      sourceStates.put(source, ReceiverState.INIT);
      syncSent.put(source, new HashSet<>());
      sourceArray[index++] = source;
    }
  }

  /**
   * Swap the messages to the ready queue
   *
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
   *
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

  /**
   * Check weather all the other information is flushed
   *
   * @return true if there is nothing to process
   */
  protected boolean isAllEmpty() {
    for (int i = 0; i < targets.length; i++) {
      Queue<Object> msgQueue = messages.get(targets[i]);
      if (msgQueue.size() > 0) {
        return false;
      }

      List<Object> queue = readyToSend.get(targets[i]);
      if (queue != null && queue.size() > 0) {
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
        || sourceStates.get(source) == ReceiverState.SYNCED) {
      return false;
    }

    if (sourceStates.get(source) == ReceiverState.INIT) {
      sourceStates.put(source, ReceiverState.RECEIVING);
    }

    Queue<Object> msgQueue = messages.get(target);
    List<Object> readyQueue = readyToSend.get(target);
    int size = msgQueue.size();
    if (readyQueue != null) {
      size += readyQueue.size();
    }

    return size < highWaterMark;
  }

  @Override
  public void onFinish(int source) {
    addSyncMessage(source, 0);
  }

  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;

    if (lock.tryLock()) {
      try {
        boolean allEmpty = true;
        for (int i = 0; i < targets.length; i++) {
          int key = targets[i];
          Queue<Object> val = messages.get(key);

          if (val != null && val.size() > 0) {
            merge(key, val);
          }

          // check weather we are ready to send and we have values to send
          if (!isFilledToSend(key)) {
            continue;
          }

          // if we send this list successfully
          if (!sendToTarget(representSource, key)) {
            needsFurtherProgress = true;
          }
          allEmpty &= val.isEmpty();
        }

        if (!allEmpty || !isAllEmpty() || !sync()) {
          needsFurtherProgress = true;
        }
      } finally {
        lock.unlock();
      }
    }
    return needsFurtherProgress;
  }

  /**
   * Handle the sync
   *
   * @return true if everything is synced
   */
  public boolean sync() {
    boolean allSyncsSent = true;
    boolean allSynced = true;
    for (int i = 0; i < sourceArray.length; i++) {
      ReceiverState state = sourceStates.get(sourceArray[i]);
      if (state == ReceiverState.RECEIVING) {
        return false;
      }

      if (state != ReceiverState.INIT && state != ReceiverState.SYNCED) {
        allSynced = false;
      }
    }

    if (allSynced) {
      return true;
    }

    for (int i = 0; i < thisSourceArray.length; i++) {
      int source = thisSourceArray[i];
      Set<Integer> finishedDestPerSource = syncSent.get(source);
      for (int j = 0; j < targets.length; j++) {
        int dest = targets[j];
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
            allSyncsSent = false;
            // no point in going further
            break;
          }
        }
      }
    }

    return allSyncsSent;
  }

  @Override
  public void clean() {
    for (int t : thisDestinations) {
      clearTarget(t);
    }

    for (Map.Entry<Integer, Set<Integer>> e : syncSent.entrySet()) {
      e.getValue().clear();
    }

    for (int source : thisSources) {
      sourceStates.put(source, ReceiverState.INIT);
    }
    syncState = SyncState.SYNC;
    barriers.clear();
    stateCleared = false;
  }
}
