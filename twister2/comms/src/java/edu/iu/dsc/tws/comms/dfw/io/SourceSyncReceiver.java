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
   * Weather this target flushed all its values after the sync
   */
  protected Map<Integer, Boolean> flushedAfterSync = new HashMap<>();

  /**
   * Weather sync messages are forwarded from the partial receivers
   */
  protected Map<Integer, Boolean> isSyncSent = new HashMap<>();

  /**
   * The number of items to buffer before sending
   */
  protected int bufferCount = 1;

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
      flushedAfterSync.put(e.getKey(), false);
      isSyncSent.put(e.getKey(), false);
    }
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    Map<Integer, Boolean> syncsPerTarget = syncReceived.get(target);
    if ((flags & MessageFlags.END) == MessageFlags.END) {
      syncsPerTarget.put(source, true);
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
      }
    }
    return true;
  }

  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    for (int target : messages.keySet()) {
      // if we haven't sent the sync lets send it
      if (flushedAfterSync.get(target) && !isSyncSent.get(target)) {
        if (operation.isDelegateComplete() && operation.sendPartial(target,
            new byte[0], MessageFlags.END, destination)) {
          isSyncSent.put(target, true);
          // at this point we call the sync event
          onSyncEvent(target);
          // clear the values
          clearTarget(target);
        } else {
          needsFurtherProgress = true;
        }
        continue;
      }

      // now check weather we have the messages for this source
      Map<Integer, Queue<Object>> messagePerTarget = messages.get(target);
      Map<Integer, Boolean> finishedForTarget = syncReceived.get(target);
      boolean canProgress = true;

      while (canProgress) {
        boolean allValuesFound = true;
        boolean allSyncsPresent = true;

        boolean moreThanOne = false;
        for (Map.Entry<Integer, Queue<Object>> sourceQueues : messagePerTarget.entrySet()) {
          if (sourceQueues.getValue().size() == 0
              && !finishedForTarget.get(sourceQueues.getKey())) {
            allValuesFound = false;
            canProgress = false;
          } else if (sourceQueues.getValue().size() > 0) {
            moreThanOne = true;
          }
          // we need to check weather there is a sync for all the sources
          if (!finishedForTarget.get(sourceQueues.getKey())) {
            allSyncsPresent = false;
          }
        }

        // if we have queues with 0 and more than zero we need further communicationProgress
        if (!allValuesFound && moreThanOne) {
          needsFurtherProgress = true;
        }

        if (allValuesFound) {
          if (!aggregate(target, allSyncsPresent)) {
            needsFurtherProgress = true;
          }
        }

        if (isFilledToSend(target)) {
          if (!sendToTarget(target, allSyncsPresent)) {
            canProgress = false;
            needsFurtherProgress = true;
          }
        } else {
          needsFurtherProgress = true;
        }

        if (allSyncsPresent && allQueuesEmpty(messagePerTarget)
            && operation.isDelegateComplete()) {
          flushedAfterSync.put(target, true);
          if (operation.sendPartial(target, new byte[0],
              MessageFlags.END, destination)) {
            isSyncSent.put(target, true);
            // at this point we call the sync event
            onSyncEvent(target);
            // clear the values
            clearTarget(target);
          } else {
            needsFurtherProgress = true;
          }
        } else {
          needsFurtherProgress = true;
        }
      }
    }
    return needsFurtherProgress;
  }

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
    flushedAfterSync.put(target, false);

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
  protected abstract boolean aggregate(int target, boolean sync);

  /**
   * Return true if we are filled to send
   *
   * @return true if we are filled enough to send
   */
  protected abstract boolean isFilledToSend(int target);

  /**
   * Check weather there is nothing left in the queues
   * @param messagePerTarget the queues for the target
   * @return true if no values left
   */
  protected boolean allQueuesEmpty(Map<Integer, Queue<Object>> messagePerTarget) {
    for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
      Queue<Object> valueList = e.getValue();
      if (valueList.size() > 1) {
        return false;
      }
    }
    return true;
  }
}
