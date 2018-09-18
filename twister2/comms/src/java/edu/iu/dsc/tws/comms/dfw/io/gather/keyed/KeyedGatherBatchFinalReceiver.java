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
package edu.iu.dsc.tws.comms.dfw.io.gather.keyed;

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
import edu.iu.dsc.tws.data.memory.OperationMemoryManager;

public class KeyedGatherBatchFinalReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(KeyedGatherBatchFinalReceiver.class.getName());

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
  private boolean isStoreBased;
  private Map<Integer, OperationMemoryManager> memoryManagers;

  public KeyedGatherBatchFinalReceiver(BulkReceiver bulkReceiver) {
    this.bulkReceiver = bulkReceiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    executor = op.getTaskPlan().getThisExecutor();
    sendPendingMax = DataFlowContext.sendPendingMax(cfg);
    isStoreBased = false;
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
    this.memoryManagers = new HashMap<>();
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
//      LOG.info(String.format("%d Final add FALSE target %d source %d", executor, target, source));
    } else {
//      LOG.info(String.format("%d Final add TRUE target %d source %d", executor, target, source));
      if (object instanceof ChannelMessage) {
        ((ChannelMessage) object).incrementRefCount();
        //TODO: how to handle refcount with store based data, is it needed?
      } else if (object instanceof OperationMemoryManager) {
        isStoreBased = true;
        memoryManagers.put(target, (OperationMemoryManager) object);
      }

      Integer c = counts.get(target).get(source);
      counts.get(target).put(source, c + 1);

      if (!isStoreBased) {
        m.add(object);
      }

      if ((flags & MessageFlags.LAST) == MessageFlags.LAST) {
//        LOG.info(String.format("%d Final LAST target %d source %d", executor, target, source));
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
      Map<Integer, Integer> countMap = counts.get(t);
//      LOG.info(String.format("%d gather final counts %d %s %s", executor, t, countMap,
//          finishedForTarget));
      if (!isStoreBased) {
        boolean found = true;
        boolean moreThanOne = false;
        for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
          if (e.getValue().size() == 0 && !finishedForTarget.get(e.getKey())) {
            found = false;
          } else if (e.getValue().size() > 0) {
            moreThanOne = true;
          }

          if (!finishedForTarget.get(e.getKey())) {
            allFinished = false;
          } /*else {
          LOG.info(String.format("%d final finished receiving to %d from %d",
              executor, t, e.getKey()));
        }*/
        }

        // if we have queues with 0 and more than zero we need further progress
        if (!found && moreThanOne) {
          needsFurtherProgress = true;
        }

        if (found) {
          List<Object> out = new ArrayList<>();
          if (allFinished && dataFlowOperation.isDelegeteComplete()) {
            for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
              Queue<Object> valueList = e.getValue();
              //Add all values
              while (valueList.size() > 0) {
                Object value = valueList.poll();
                if (value instanceof List) {
                  out.addAll((List) value);
                } else {
                  out.add(value);
                }
                allFinished = false;
                needsFurtherProgress = true;
              }
            }
          } else {
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
                needsFurtherProgress = true;
              }
            }
          }
          finalMessages.get(t).addAll(out);
        }

      } else {

        for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
          if (!finishedForTarget.get(e.getKey())) {
            allFinished = false;
          }
        }
      }


      if (allFinished) {
//        LOG.info(String.format("%d final all finished %d", executor, t));
        batchDone.put(t, true);
        if (!isStoreBased) {
          bulkReceiver.receive(t, finalMessages.get(t).iterator());
        } else {
          bulkReceiver.receive(t, memoryManagers.get(t).iterator());
        }
      }
    }
    return needsFurtherProgress;
  }

  public boolean isStoreBased() {
    return isStoreBased;
  }

  public void setStoreBased(boolean storeBased) {
    isStoreBased = storeBased;
  }

}
