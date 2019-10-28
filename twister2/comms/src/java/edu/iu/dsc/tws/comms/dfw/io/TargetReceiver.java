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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.CommunicationContext;
import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.messaging.ChannelMessage;
import edu.iu.dsc.tws.api.comms.messaging.MessageFlags;
import edu.iu.dsc.tws.api.comms.messaging.MessageReceiver;
import edu.iu.dsc.tws.api.config.Config;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

public abstract class TargetReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(TargetReceiver.class.getName());

  /**
   * Lets keep track of the messages, we need to keep track of the messages for each target
   * and source, Map<target, Queue<messages>>
   */
  protected Int2ObjectOpenHashMap<List<Object>> messages = new Int2ObjectOpenHashMap<>();

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
  protected int highWaterMark = 16;

  /**
   * The lock
   */
  protected Lock lock = new ReentrantLock();

  /**
   * Sources we are expecting messages from
   */
  protected Set<Integer> thisSources;

  /**
   * The destinations we are sending messages to
   */
  protected Set<Integer> thisDestinations;

  /**
   * The sync state
   */
  protected SyncState syncState = SyncState.SYNC;

  /**
   * The message grouping size
   */
  protected long groupingSize = 100;

  /**
   * The targets
   */
  protected int[] targets;

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    workerId = op.getLogicalPlan().getThisWorker();
    operation = op;
    lowWaterMark = CommunicationContext.getNetworkPartitionMessageGroupLowWaterMark(cfg);
    highWaterMark = CommunicationContext.getNetworkPartitionMessageGroupHighWaterMark(cfg);
    this.groupingSize = CommunicationContext.getNetworkPartitionBatchGroupingSize(cfg);
    if (highWaterMark - lowWaterMark <= groupingSize) {
      groupingSize = highWaterMark - lowWaterMark - 1;
      LOG.fine("Changing the grouping size to: " + groupingSize);
    }

    Set<Integer> tars = op.getTargets();
    targets = new int[tars.size()];
    int index = 0;
    for (int t : tars) {
      targets[index++] = t;
    }
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    if (lock.tryLock()) {
      try {
        if (!representSourceSet) {
          this.representSource = source;
          representSourceSet = true;
        }

        // if we have a sync from this source we cannot accept more data
        // until we finish this sync
        if (!canAcceptMessage(source, target)) {
          return false;
        }

        if ((flags & MessageFlags.SYNC_EMPTY) == MessageFlags.SYNC_EMPTY) {
          addSyncMessage(source, target);
          return true;
        } else if ((flags & MessageFlags.SYNC_BARRIER) == MessageFlags.SYNC_BARRIER) {
          addSyncMessageBarrier(source, target, (byte[]) object);
          return true;
        }

        if (object instanceof ChannelMessage) {
          ((ChannelMessage) object).incrementRefCount();
        }

        List<Object> msgQueue = messages.get(target);
        addMessage(target, msgQueue, object);

        if (msgQueue.size() > lowWaterMark) {
          merge(target, msgQueue);
        }

        if ((flags & MessageFlags.SYNC_MESSAGE) == MessageFlags.SYNC_MESSAGE) {
          addSyncMessage(source, target);
        }

        return true;
      } finally {
        lock.unlock();
      }
    }
    return false;
  }

  protected void addMessage(int target, List<Object> msgQueue, Object value) {
    if (value instanceof AggregatedObjects) {
      msgQueue.addAll((Collection<?>) value);
    } else {
      msgQueue.add(value);
    }
  }

  /**
   * Add a sync message
   *
   * @param source source
   * @param target target
   */
  protected abstract void addSyncMessage(int source, int target);

  /**
   * Add a sync message
   *
   * @param source source
   * @param target target
   * @param barrier the barrier message
   */
  protected abstract void addSyncMessageBarrier(int source, int target, byte[] barrier);

  /**
   * Check weather we can accept a message
   *
   * @param source source
   * @param target target
   */
  protected abstract boolean canAcceptMessage(int source, int target);

  /**
   * Swap the messages to the ready queue
   *
   * @param dest the target
   * @param dests message queue to switch to ready
   */
  protected abstract void merge(int dest, List<Object> dests);



  /**
   * Clear all the buffers for the target, to ready for the next
   *
   * @param target target
   */
  protected void clearTarget(int target) {
    List<Object> messagesPerTarget = messages.get(target);
    messagesPerTarget.clear();
  }

  /**
   * Send the values to a target
   *
   * @param source the sources
   * @param target the target
   * @return true if all the values are sent successfully
   */
  protected abstract boolean sendToTarget(int source, int target);

  /**
   * This method is called when there is a sync event on the operation
   * @param target the target to which the sync event belong
   * @param value the byte value, can be null
   */
  protected abstract boolean onSyncEvent(int target, byte[] value);

  /**
   * Return true if we are filled to send
   *
   * @return true if we are filled enough to send
   */
  protected abstract boolean isFilledToSend(int target);
}
