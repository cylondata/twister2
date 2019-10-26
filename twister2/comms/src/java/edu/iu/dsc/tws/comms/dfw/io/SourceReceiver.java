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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.messaging.ChannelMessage;
import edu.iu.dsc.tws.api.comms.messaging.MessageFlags;
import edu.iu.dsc.tws.api.comms.messaging.MessageReceiver;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;

public abstract class SourceReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(SourceReceiver.class.getName());

  /**
   * Lets keep track of the messages, we need to keep track of the messages for each target
   * and source, Map<target, map<source, Queue<messages>>
   */
  protected Map<Integer, Map<Integer, Queue<Object>>> messages = new HashMap<>();

  /**
   * The worker id this receiver is in
   */
  protected int workerId;

  /**
   * The operations
   */
  protected DataFlowOperation operation;

  /**
   * The pending max per source
   */
  protected int sendPendingMax;

  /**
   * The destination
   */
  protected int destination;

  /**
   * Keep weather we have received a sync from a source
   */
  protected Map<Integer, Set<Integer>> syncReceived = new HashMap<>();

  /**
   * Number of sources per target
   */
  protected Map<Integer, Integer> sourcesOfTarget = new HashMap<>();

  /**
   * Weather sync messages are forwarded from the partial receivers
   */
  protected Map<Integer, Boolean> isSyncSent = new HashMap<>();

  /**
   * Keep state about the targets
   */
  protected Map<Integer, ReceiverState> targetStates = new HashMap<>();

  /**
   * The barriers for each target
   */
  protected Map<Integer, byte[]> barriers = new HashMap<>();

  /**
   * The sync state
   */
  protected SyncState syncState = SyncState.SYNC;

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    workerId = op.getLogicalPlan().getThisWorker();
    sendPendingMax = CommunicationContext.sendPendingMax(cfg);
    this.operation = op;

    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      Map<Integer, Queue<Object>> messagesPerTask = new HashMap<>();

      for (int task : e.getValue()) {
        messagesPerTask.put(task, new ArrayBlockingQueue<>(sendPendingMax));
      }
      syncReceived.put(e.getKey(), new HashSet<>());
      messages.put(e.getKey(), messagesPerTask);
      isSyncSent.put(e.getKey(), false);
      sourcesOfTarget.put(e.getKey(), e.getValue().size());
      targetStates.put(e.getKey(), ReceiverState.INIT);
    }
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    Set<Integer> syncsPerTarget = syncReceived.get(target);

    if ((flags & MessageFlags.SYNC_EMPTY) == MessageFlags.SYNC_EMPTY) {
      if (syncState == SyncState.BARRIER_SYNC) {
        throw new RuntimeException("We are receiving barriers syncs and received a normal sycn");
      }
      syncsPerTarget.add(source);
      if (allSyncsPresent(target)) {
        targetStates.put(target, ReceiverState.ALL_SYNCS_RECEIVED);
      }
      return true;
    } else if ((flags & MessageFlags.SYNC_BARRIER) == MessageFlags.SYNC_BARRIER) {
      syncsPerTarget.add(source);
      if (allSyncsPresent(target)) {
        targetStates.put(target, ReceiverState.ALL_SYNCS_RECEIVED);
      }
      syncState = SyncState.BARRIER_SYNC;

      if (object instanceof Tuple) {
        barriers.put(target, (byte[]) ((Tuple) object).getValue());
      } else {
        barriers.put(target, (byte[]) object);
      }
      return true;
    }

    if (targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED
        || targetStates.get(target) == ReceiverState.SYNCED) {
      return false;
    }

    if (targetStates.get(target) == ReceiverState.INIT) {
      targetStates.put(target, ReceiverState.RECEIVING);
    }

    // if we have a sync from this source we cannot accept more data
    // until we finish this sync
    if (syncsPerTarget.contains(source)) {
      return false;
    }

    Queue<Object> msgQueue = messages.get(target).get(source);
    if (msgQueue.size() >= sendPendingMax) {
      return false;
    } else {
      if (object instanceof ChannelMessage) {
        ((ChannelMessage) object).incrementRefCount();
      }

      msgQueue.add(object);
      if ((flags & MessageFlags.SYNC_MESSAGE) == MessageFlags.SYNC_MESSAGE) {
        syncsPerTarget.add(source);
        if (allSyncsPresent(target)) {
          targetStates.put(target, ReceiverState.ALL_SYNCS_RECEIVED);
        }
      }
    }
    return true;
  }

  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    for (int target : messages.keySet()) {
      // if we are at init state nothing to do
      if (targetStates.get(target) == ReceiverState.SYNCED) {
        continue;
      }
      // now check weather we have the messages for this source
      Map<Integer, Queue<Object>> messagePerTarget = messages.get(target);
      Set<Integer> finishedForTarget = syncReceived.get(target);
      boolean canProgress = true;

      while (canProgress) {
        boolean allValuesFound = true;
        boolean allSyncsPresent = true;
        boolean anyValuesFound = false;

        for (Map.Entry<Integer, Queue<Object>> sourceQueues : messagePerTarget.entrySet()) {
          if (sourceQueues.getValue().size() == 0) {
            allValuesFound = false;
            canProgress = false;
          } else {
            anyValuesFound = true;
          }

          // we need to check weather there is a sync for all the sources
          if (!finishedForTarget.contains(sourceQueues.getKey())) {
            allSyncsPresent = false;
          }
        }

        // if we have found all the values from sources or if syncs are present
        // we need to aggregate
        if (anyValuesFound) {
          aggregate(target, allSyncsPresent, allValuesFound);
        }

        // if we are filled to send, lets send the values
        if (isFilledToSend(target, allSyncsPresent)) {
          if (!sendToTarget(target, allSyncsPresent)) {
            canProgress = false;
          }
        }

        // finally if there is a sync, and all queues are empty
        if (targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED
            && allQueuesEmpty(messagePerTarget)
            && isAllEmpty(target)) {
          needsFurtherProgress = sendSyncForward(needsFurtherProgress, target);
          if (!needsFurtherProgress) {
            targetStates.put(target, ReceiverState.SYNCED);
            // at this point we call the sync event
            onSyncEvent(target, barriers.get(target));
            clearTarget(target);
          }
        }
      }

      needsFurtherProgress = targetStates.get(target) != ReceiverState.SYNCED;
    }
    return needsFurtherProgress;
  }

  @Override
  public boolean isComplete() {
    return false;
  }

  /**
   * Check weather all the other information is flushed
   *
   * @param target target
   * @return true if there is nothing to process
   */
  protected abstract boolean isAllEmpty(int target);

  /**
   * Handle the sync
   *
   * @param needsFurtherProgress the current value of need progress
   * @param target target
   * @return the needFurtherProgress
   */
  protected abstract boolean sendSyncForward(boolean needsFurtherProgress, int target);

  /**
   * Clear all the buffers for the target, to ready for the next
   *
   * @param target target
   */
  private void clearTarget(int target) {
    Map<Integer, Queue<Object>> messagesPerTarget = messages.get(target);
    for (Map.Entry<Integer, Queue<Object>> mEntry : messagesPerTarget.entrySet()) {
      mEntry.getValue().clear();
    }
    isSyncSent.put(target, false);
    syncState = SyncState.SYNC;
    syncReceived.forEach((k, v) -> v.clear());
  }

  /**
   * Send the values to a target
   *
   * @param target the target
   * @return true if all the values are sent successfully
   */
  protected abstract boolean sendToTarget(int target, boolean sync);

  /**
   * Aggregate values from sources for a target, assumes every source has a value
   *
   * @param target target
   * @param sync true if all the syncs are present
   * @return true if there are no more elements to aggregate
   */
  protected abstract boolean aggregate(int target, boolean sync, boolean allValuesFound);

  /**
   * Return true if we are filled to send
   *
   * @return true if we are filled enough to send
   */
  protected abstract boolean isFilledToSend(int target, boolean sync);

  /**
   * Check weather there is nothing left in the queues
   *
   * @param messagePerTarget the queues for the target
   * @return true if no values left
   */
  protected boolean allQueuesEmpty(Map<Integer, Queue<Object>> messagePerTarget) {
    for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
      Queue<Object> valueList = e.getValue();
      if (valueList.size() > 0) {
        return false;
      }
    }
    return true;
  }

  /**
   * Checks weather all syncs are present
   *
   * @return true if all syncs are present
   */
  protected boolean allSyncsPresent(int target) {
    return sourcesOfTarget.get(target) == syncReceived.get(target).size();
  }

  protected void onFinish(int source) {
    for (Integer target : syncReceived.keySet()) {
      syncReceived.get(target).add(source);
    }
  }

  @Override
  public void clean() {
    for (int target : messages.keySet()) {
      clearTarget(target);
      targetStates.put(target, ReceiverState.INIT);
    }
  }

  /**
   * This method is called when there is a sync event on the operation
   * @param target the target to which the sync event belong
   * @param value the byte value, can be null
   */
  protected abstract void onSyncEvent(int target, byte[] value);
}
