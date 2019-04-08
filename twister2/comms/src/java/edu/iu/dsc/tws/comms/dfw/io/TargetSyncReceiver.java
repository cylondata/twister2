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

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;

public abstract class TargetSyncReceiver implements MessageReceiver {
  /**
   * Lets keep track of the messages, we need to keep track of the messages for each target
   * and source, Map<target, map<source, Queue<messages>>
   */
  protected Map<Integer, Queue<Object>> messages = new HashMap<>();

  /**
   * The worker id this receiver is in
   */
  protected int workerId;

  /**
   * The operations
   */
  protected DataFlowOperation operation;

  /**
   * The destination
   */
  protected int destination;

  /**
   * Keep weather we have received a sync from a source
   */
  protected Map<Integer, Map<Integer, Boolean>> syncReceived = new HashMap<>();

  /**
   * Keep the list of tuple [Object, Source, Flags] for each destination
   */
  protected Map<Integer, List<Object>> readyToSend = new HashMap<>();

  /**
   * Weather sync messages are forwarded from the partial receivers
   */
  protected Map<Integer, Boolean> isSyncSent = new HashMap<>();

  /**
   * Keep state about the targets
   */
  protected Map<Integer, ReceiverState> targetStates = new HashMap<>();

  /**
   * The source task connected to this partial receiver
   */
  protected int representSource;

  /**
   * Keep weather source is set
   */
  private boolean representSourceSet = false;

  /**
   * Low water mark
   */
  private int lowWaterMark = 8;

  /**
   * High water mark to keep track of objects
   */
  private int highWaterMark = 16;

  /**
   * The lock
   */
  private Lock lock = new ReentrantLock();

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    workerId = op.getTaskPlan().getThisExecutor();
    this.operation = op;
    lowWaterMark = DataFlowContext.getNetworkPartitionMessageGroupLowWaterMark(cfg);
    highWaterMark = DataFlowContext.getNetworkPartitionMessageGroupHighWaterMark(cfg);

    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      Queue<Object> messagesPerTask = new LinkedBlockingQueue<>();
      Map<Integer, Boolean> finishedPerTask = new HashMap<>();

      for (int task : e.getValue()) {
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
    lock.lock();
    try {
      if (!representSourceSet) {
        this.representSource = source;
        representSourceSet = true;
      }

      Map<Integer, Boolean> syncsPerTarget = syncReceived.get(target);
      if ((flags & MessageFlags.END) == MessageFlags.END) {
        syncsPerTarget.put(source, true);
        if (allSyncsPresent(syncsPerTarget)) {
          targetStates.put(target, ReceiverState.ALL_SYNCS_RECEIVED);
        }
        return true;
      }

      // if we have a sync from this source we cannot accept more data
      // until we finish this sync
      if (targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED
          || targetStates.get(target) == ReceiverState.SYNCED) {
        return false;
      }

      Queue<Object> msgQueue = messages.get(target);
      if (msgQueue.size() >= highWaterMark) {
        return false;
      }

      if (object instanceof ChannelMessage) {
        ((ChannelMessage) object).incrementRefCount();
      }

      if (object instanceof AggregatedObjects) {
        msgQueue.addAll((Collection<?>) object);
      } else {
        msgQueue.add(object);
      }

      if (msgQueue.size() > lowWaterMark) {
        swapToReady(target, msgQueue);
      }

      if ((flags & MessageFlags.LAST) == MessageFlags.LAST) {
        syncsPerTarget.put(source, true);
        if (allSyncsPresent(syncsPerTarget)) {
          targetStates.put(target, ReceiverState.ALL_SYNCS_RECEIVED);
        }
      }
      return true;
    } finally {
      lock.unlock();
    }
  }

  protected void swapToReady(int dest, Queue<Object> dests) {
    if (!readyToSend.containsKey(dest)) {
      readyToSend.put(dest, new AggregatedObjects<>(dests));
    } else {
      List<Object> ready = readyToSend.get(dest);
      ready.addAll(dests);
    }
    dests.clear();
  }

  @Override
  public boolean progress() {
    lock.lock();
    try {
      boolean needsFurtherProgress = false;
      for (int target : messages.keySet()) {
        // now check weather we have the messages for this source
        Queue<Object> messagePerTarget = messages.get(target);
        Map<Integer, Boolean> finishedForTarget = syncReceived.get(target);
        boolean canProgress = true;

        while (canProgress) {
          boolean allSyncsPresent = allSyncsPresent(finishedForTarget);

          // if we have found all the values from sources or if syncs are present
          // we need to aggregate
          if (messagePerTarget.size() > 0) {
            aggregate(target, allSyncsPresent);
          } else {
            canProgress = false;
          }

          // if we are filled to send, lets send the values
          if (isFilledToSend(target, allSyncsPresent)) {
            if (!sendToTarget(target, allSyncsPresent)) {
              canProgress = false;
            }
          } else {
            canProgress = false;
          }

          // finally if there is a sync, and all queues are empty
          if (targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED
              && allQueuesEmpty(messagePerTarget)
              && isAllEmpty(target)
              && operation.isDelegateComplete()) {
            needsFurtherProgress = sendSyncForward(needsFurtherProgress, target);
            if (!needsFurtherProgress) {
              targetStates.put(target, ReceiverState.SYNCED);
              // at this point we call the sync event
              onSyncEvent(target);
              clearTarget(target);
            }
          }
        }

        needsFurtherProgress = targetStates.get(target) != ReceiverState.SYNCED;
      }
      return needsFurtherProgress;
    } finally {
      lock.unlock();
    }
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
    Queue<Object> messagesPerTarget = messages.get(target);
    messagesPerTarget.clear();

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
  protected abstract boolean aggregate(int target, boolean sync);

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
  protected boolean allQueuesEmpty(Queue<Object> messagePerTarget) {
    return messagePerTarget.size() == 0;
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
