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
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;

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

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    this.workerId = op.getTaskPlan().getThisExecutor();
    this.operation = op;
    this.sendPendingMax = DataFlowContext.sendPendingMax(cfg);

    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      Map<Integer, Queue<Object>> messagesPerTask = new HashMap<>();
      for (int i : e.getValue()) {
        messagesPerTask.put(i, new ArrayBlockingQueue<>(sendPendingMax));
      }
      LOG.fine(String.format("%d Final Task %d receives from %s",
          workerId, e.getKey(), e.getValue().toString()));
      messages.put(e.getKey(), messagesPerTask);
    }
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    boolean canAdd = true;
    Queue<Object> messagePerTask = messages.get(target).get(source);
    if (messagePerTask.size() >= sendPendingMax) {
      canAdd = false;
    } else {
      // we need to increment the reference count to make the buffers available
      // other wise they will bre reclaimed
      if (object instanceof ChannelMessage) {
        ((ChannelMessage) object).incrementRefCount();
      }

      messagePerTask.offer(object);
    }
    return canAdd;
  }

  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    for (int target : messages.keySet()) {
      boolean canProgress = true;

      Map<Integer, Queue<Object>> messagePerTarget = messages.get(target);

      while (canProgress) {
        boolean found = true;
        boolean moreThanOne = false;

        // now check weather we have the messages for each source
        for (Map.Entry<Integer, Queue<Object>> sourceEntry : messagePerTarget.entrySet()) {
          if (sourceEntry.getValue().size() == 0) {
            found = false;
            canProgress = false;
          } else {
            moreThanOne = true;
          }
        }

        // if we have queues with 0 and more than zero we need further communicationProgress
        if (!found && moreThanOne) {
          needsFurtherProgress = true;
        }

        if (found) {
          if (!aggregate(target)) {
            needsFurtherProgress = true;
          }
        }

        if (!sendToTarget(target)) {
          canProgress = false;
          needsFurtherProgress = true;
        }
      }
    }
    return needsFurtherProgress;
  }

  /**
   * Send the values to a target
   * @param target the target
   * @return true if all the values are sent successfully
   */
  protected abstract boolean sendToTarget(int target);

  /**
   * Aggregate values from sources for a target, assumes every source has a value
   * @param target target
   * @return true if there are no more elements to aggregate
   */
  protected abstract boolean aggregate(int target);
}
