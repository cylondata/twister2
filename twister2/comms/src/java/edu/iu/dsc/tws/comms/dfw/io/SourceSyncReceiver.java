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
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;

public abstract class SourceSyncReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(SourceSyncReceiver.class.getName());

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
  protected Map<Integer, Map<Integer, Boolean>> syncReceived = new HashMap<>();

  /**
   * Weather sync messages are forwarded from the partial receivers
   */
  protected Map<Integer, Boolean> isSyncSent = new HashMap<>();

  /**
   * The number of items to buffer before sending
   */
  protected int bufferCount = 1;

  protected enum ReceiverState {
    // we are in the receiving state initially
    RECEIVING,
    // we starts to receive syncs
    SYNC_RECEIVING,
    // all the syncs required are received
    ALL_SYNCS_RECEIVED,
  }

  protected Map<Integer, ReceiverState> targetStates = new HashMap<>();

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    workerId = op.getTaskPlan().getThisExecutor();
    sendPendingMax = DataFlowContext.sendPendingMax(cfg);
    this.operation = op;

    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      Map<Integer, Queue<Object>> messagesPerTask = new HashMap<>();
      Map<Integer, Boolean> finishedPerTask = new HashMap<>();

      for (int task : e.getValue()) {
        messagesPerTask.put(task, new ArrayBlockingQueue<>(sendPendingMax));
        finishedPerTask.put(task, false);
      }
      messages.put(e.getKey(), messagesPerTask);
      syncReceived.put(e.getKey(), finishedPerTask);
      isSyncSent.put(e.getKey(), false);
      targetStates.put(e.getKey(), ReceiverState.RECEIVING);
    }
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    Map<Integer, Boolean> syncsPerTarget = syncReceived.get(target);
    if ((flags & MessageFlags.END) == MessageFlags.END) {
      syncsPerTarget.put(source, true);
      if (allSyncsPresent(syncsPerTarget)) {
        targetStates.put(target, ReceiverState.ALL_SYNCS_RECEIVED);
      } else {
        targetStates.put(target, ReceiverState.SYNC_RECEIVING);
      }
      return true;
    }

    // if we have a sync from this source we cannot accept more data
    // until we finish this sync
    if (syncsPerTarget.get(source)) {
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
      if ((flags & MessageFlags.LAST) == MessageFlags.LAST) {
        syncsPerTarget.put(source, true);
        if (allSyncsPresent(syncsPerTarget)) {
          targetStates.put(target, ReceiverState.ALL_SYNCS_RECEIVED);
        } else {
          targetStates.put(target, ReceiverState.SYNC_RECEIVING);
        }
      }
    }
    return true;
  }

  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    for (int target : messages.keySet()) {
      // now check weather we have the messages for this source
      Map<Integer, Queue<Object>> messagePerTarget = messages.get(target);
      Map<Integer, Boolean> finishedForTarget = syncReceived.get(target);
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
          if (!finishedForTarget.get(sourceQueues.getKey())) {
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
            && isAllEmpty(target)
            && operation.isDelegateComplete()) {
          needsFurtherProgress = sendSyncForward(needsFurtherProgress, target);
          if (!needsFurtherProgress) {
            LOG.info("Synced");
            // at this point we call the sync event
            onSyncEvent(target);
            clearTarget(target);
            targetStates.put(target, ReceiverState.RECEIVING);
          }
        }
      }

      needsFurtherProgress = !allQueuesEmpty(messagePerTarget)
          || !isAllEmpty(target) || !syncsEmpty(target);
    }
    return needsFurtherProgress;
  }

  private boolean syncsEmpty(int target) {
    Map<Integer, Boolean> syncs = syncReceived.get(target);
    for (Boolean b : syncs.values()) {
      if (b) {
        return true;
      }
    }
    return !isSyncSent.get(target);
  }

  /**
   * Check weather all the other information is flushed
   * @param target target
   * @return true if there is nothing to process
   */
  protected abstract boolean isAllEmpty(int target);

  /**
   * Handle the sync
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

    Map<Integer, Boolean> syncs = syncReceived.get(target);
    for (Integer source : syncs.keySet()) {
      syncs.put(source, false);
    }
  }

  /**
   * Send the values to a target
   * @param target the target
   * @return true if all the values are sent successfully
   */
  protected abstract boolean sendToTarget(int target, boolean sync);

  /**
   * Aggregate values from sources for a target, assumes every source has a value
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
   * @param syncs the syncs map
   * @return true if all syncs are present
   */
  protected boolean allSyncsPresent(Map<Integer, Boolean> syncs) {
    for (Boolean sync : syncs.values()) {
      if (!sync) {
        return false;
      }
    }
    return true;
  }

  @Override
  public void onFinish(int source) {
    for (Integer target : syncReceived.keySet()) {
      Map<Integer, Boolean> finishedMessages = syncReceived.get(target);
      finishedMessages.put(source, true);
    }
  }
}
