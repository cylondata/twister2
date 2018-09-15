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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;

public class GatherBatchFinalReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(GatherBatchFinalReceiver.class.getName());

  // lets keep track of the messages
  // for each task we need to keep track of incoming messages
  private Map<Integer, Map<Integer, Queue<Object>>> messages = new HashMap<>();
  private Map<Integer, Map<Integer, Boolean>> finished = new HashMap<>();
  private Map<Integer, List<Object>> finalMessages = new HashMap<>();
  private Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();
  private DataFlowOperation dataFlowOperation;
  private int executor;
  private int sendPendingMax = 128;
  private BulkReceiver bulkReceiver;
  private Map<Integer, Boolean> batchDone = new HashMap<>();

  public GatherBatchFinalReceiver(BulkReceiver bulkReceiver) {
    this.bulkReceiver = bulkReceiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    executor = op.getTaskPlan().getThisExecutor();
    sendPendingMax = DataFlowContext.sendPendingMax(cfg);
    LOG.fine(String.format("%d expected ids %s", executor, expectedIds));
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
      finalMessages.put(e.getKey(), new ArrayList<>());
      counts.put(e.getKey(), countsPerTask);
      batchDone.put(e.getKey(), false);
    }
    this.dataFlowOperation = op;
    this.executor = dataFlowOperation.getTaskPlan().getThisExecutor();
    this.bulkReceiver.init(cfg, expectedIds.keySet());
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    // add the object to the map
    boolean canAdd = true;
    Queue<Object> m = messages.get(target).get(source);
    Map<Integer, Boolean> finishedMessages = finished.get(target);
    if ((flags & MessageFlags.END) == MessageFlags.END) {
      finishedMessages.put(source, true);
      return true;
    }
    if (m.size() >= sendPendingMax) {
      canAdd = false;
    } else {
      if (object instanceof ChannelMessage) {
        ((ChannelMessage) object).incrementRefCount();
        //TODO: how to handle refcount with store based data, is it needed?
      }

      Integer c = counts.get(target).get(source);
      counts.get(target).put(source, c + 1);
      m.add(object);

      if ((flags & MessageFlags.LAST) == MessageFlags.LAST) {
        finishedMessages.put(source, true);
      }
    }
    return canAdd;
  }

  /**
   * Method used to communicationProgress work
   */
  @SuppressWarnings("unchecked")
  public boolean progress() {
    boolean needsFurtherProgress = false;
    for (int t : messages.keySet()) {
      if (batchDone.get(t)) {
        continue;
      }

      boolean allFinished = true;
      // now check weather we have the messages for this source
      Map<Integer, Queue<Object>> map = messages.get(t);
      Map<Integer, Boolean> finishedForTarget = finished.get(t);

      boolean found = true;
      boolean moreThanOne = false;
      for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
        if (e.getValue().size() == 0 && !finishedForTarget.get(e.getKey())) {
          found = false;
        } else {
          moreThanOne = true;
        }

        if (!finishedForTarget.get(e.getKey())) {
          allFinished = false;
        }
      }

      // if we have queues with 0 and more than zero we need further progress
      if (!found && moreThanOne) {
        needsFurtherProgress = true;
      }

      if (found) {
        List<Object> out = new ArrayList<>();
        for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
          Queue<Object> valueList = e.getValue();
          if (valueList.size() > 0) {
            Object value = valueList.poll();
            if (value instanceof List) {
              out.addAll((List) value);
            } else {
              out.add(value);
            }
            allFinished = false;
          }
        }
        finalMessages.get(t).addAll(out);
      } else {
        allFinished = false;
      }


      if (allFinished) {
        batchDone.put(t, true);
        bulkReceiver.receive(t, finalMessages.get(t).iterator());
        // we can call on finish at this point
        onFinish(t);
      }
    }
    return needsFurtherProgress;
  }
}
