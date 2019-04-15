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
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;

public abstract class TargetSyncReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(TargetSyncReceiver.class.getName());

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
   * The source task connected to this partial receiver
   */
  private int representSource;

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
  protected int highWaterMark = 16;

  /**
   * The lock
   */
  private Lock lock = new ReentrantLock();

  /**
   * Sources we are expecting messages from
   */
  protected Set<Integer> thisSources;

  /**
   * The destinations we are sending messages to
   */
  protected Set<Integer> thisDestinations;

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    workerId = op.getTaskPlan().getThisExecutor();
    operation = op;
    lowWaterMark = DataFlowContext.getNetworkPartitionMessageGroupLowWaterMark(cfg);
    highWaterMark = DataFlowContext.getNetworkPartitionMessageGroupHighWaterMark(cfg);
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    lock.lock();
    try {
      if (!representSourceSet) {
        this.representSource = source;
        representSourceSet = true;
      }

      if ((flags & MessageFlags.END) == MessageFlags.END) {
        addSyncMessage(source, target);
        return true;
      }

      // if we have a sync from this source we cannot accept more data
      // until we finish this sync
      if (!canAcceptMessage(source, target)) {
        return false;
      }

      if (object instanceof ChannelMessage) {
        ((ChannelMessage) object).incrementRefCount();
      }

      Queue<Object> msgQueue = messages.get(target);
      addMessage(msgQueue, object);

      if (msgQueue.size() > lowWaterMark) {
        merge(target, msgQueue);
      }

      if ((flags & MessageFlags.LAST) == MessageFlags.LAST) {
        addSyncMessage(source, target);
      }

      return true;
    } finally {
      lock.unlock();
    }
  }

  protected void addMessage(Queue<Object> msgQueue, Object value) {
    if (value instanceof AggregatedObjects) {
      msgQueue.addAll((Collection<?>) value);
    } else {
      msgQueue.add(value);
    }
  }

  /**
   * Add a sync message
   * @param source source
   * @param target target
   */
  protected abstract void addSyncMessage(int source, int target);

  /**
   * Check weather we can accept a message
   * @param source source
   * @param target target
   */
  protected abstract boolean canAcceptMessage(int source, int target);

  /**
   * Swap the messages to the ready queue
   * @param dest the target
   * @param dests message queue to switch to ready
   */
  protected abstract void merge(int dest, Queue<Object> dests);

  @Override
  public synchronized boolean progress() {
    boolean needsFurtherProgress = false;

    lock.lock();
    try {
      for (Map.Entry<Integer, Queue<Object>> e : messages.entrySet()) {
        if (e.getValue().size() > 0) {
          merge(e.getKey(), e.getValue());
        }

        // check weather we are ready to send and we have values to send
        if (!isFilledToSend(e.getKey())) {
          continue;
        }

        // if we send this list successfully
        if (!sendToTarget(representSource, e.getKey())) {
          needsFurtherProgress = true;
        }
      }

      if (!isAllEmpty() || !sync() || !operation.isDelegateComplete()) {
        needsFurtherProgress = true;
      }
    } finally {
      lock.unlock();
    }

    return needsFurtherProgress;
  }

  /**
   * Handle the sync
   *
   * @return true if everything is synced
   */
  protected abstract boolean sync();

  /**
   * Check weather all the other information is flushed
   * @return true if there is nothing to process
   */
  protected boolean isAllEmpty() {
    for (Map.Entry<Integer, Queue<Object>> e : messages.entrySet()) {
      if (e.getValue().size() > 0) {
        return false;
      }
    }

    return true;
  }

  /**
   * Clear all the buffers for the target, to ready for the next
   *
   * @param target target
   */
  protected void clearTarget(int target) {
    Queue<Object> messagesPerTarget = messages.get(target);
    messagesPerTarget.clear();
  }

  /**
   * Send the values to a target
   * @param source the sources
   * @param target the target
   * @return true if all the values are sent successfully
   */
  protected abstract boolean sendToTarget(int source, int target);

  /**
   * Return true if we are filled to send
   *
   * @return true if we are filled enough to send
   */
  protected abstract boolean isFilledToSend(int target);

  @Override
  public void onFinish(int source) {
    throw new RuntimeException("Not implemented");
  }
}
