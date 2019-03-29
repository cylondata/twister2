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
package edu.iu.dsc.tws.comms.dfw.io.gather;

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

public abstract class BaseGatherBatchReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(BaseGatherBatchReceiver.class.getName());

  /**
   * for each task we need to keep track of incoming messages from different sources
   * [task, [source, queue]]
   */
  protected Map<Integer, Map<Integer, Queue<Object>>> messages = new HashMap<>();
  /**
   * We have received finish messages
   * [task, [source, bool]]
   */
  protected Map<Integer, Map<Integer, Boolean>> finished = new HashMap<>();

  /**
   * Keep track of counts for debug purposes
   * [task, [source, count]
   */
  protected Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();

  /**
   * The dataflow operation
   */
  protected DataFlowOperation dataFlowOperation;

  /**
   * Worker id
   */
  protected int workerId;

  /**
   * Number of items pending before we flush
   */
  protected int sendPendingMax = 128;

  /**
   * Configuration
   */
  protected Config config;

  /**
   * Expected sources for targets
   * [target, [sources]]
   */
  protected Map<Integer, List<Integer>> expIds;

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    workerId = op.getTaskPlan().getThisExecutor();
    sendPendingMax = DataFlowContext.sendPendingMax(cfg);
    LOG.fine(String.format("%d expected ids %s", workerId, expectedIds));
    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      Map<Integer, Queue<Object>> messagesPerTask = new HashMap<>();
      Map<Integer, Boolean> finishedPerTask = new HashMap<>();
      Map<Integer, Integer> countsPerTask = new HashMap<>();

      for (int i : e.getValue()) {
        messagesPerTask.put(i, new ArrayBlockingQueue<>(sendPendingMax));
        finishedPerTask.put(i, false);
        countsPerTask.put(i, 0);
      }
      messages.put(e.getKey(), messagesPerTask);
      finished.put(e.getKey(), finishedPerTask);
      counts.put(e.getKey(), countsPerTask);
    }
    this.dataFlowOperation = op;
    this.expIds = expectedIds;
    this.workerId = dataFlowOperation.getTaskPlan().getThisExecutor();
    // subclass specific initializations
    init();
  }

  protected abstract void init();

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    // add the object to the map
    boolean canAdd = true;

    // there is something wrong in the operation configuration
    if (messages.get(target) == null) {
      throw new RuntimeException(String.format("%d Partial receive error %d", workerId, target));
    }

    // get the queue for target and source
    Queue<Object> m = messages.get(target).get(source);
    if ((flags & MessageFlags.END) == MessageFlags.END) {
      // finished messages
      Map<Integer, Boolean> finishedMessages = finished.get(target);
      finishedMessages.put(source, true);
      return true;
    }

    // we cannot add further
    if (m.size() >= sendPendingMax) {
      canAdd = false;
    } else {
      if (object instanceof ChannelMessage) {
        ((ChannelMessage) object).incrementRefCount();
      }
      Integer c = counts.get(target).get(source);
      counts.get(target).put(source, c + 1);

      m.add(object);
      if ((flags & MessageFlags.LAST) == MessageFlags.LAST) {
        // finished messages
        Map<Integer, Boolean> finishedMessages = finished.get(target);
        finishedMessages.put(source, true);
      }
    }
    return canAdd;
  }
}
