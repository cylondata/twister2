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
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;


public class KeyedGatherBatchPartialReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(
      KeyedGatherBatchPartialReceiver.class.getName());

  // lets keep track of the messages
  // for each task we need to keep track of incoming messages
  private Map<Integer, Map<Integer, Queue<Object>>> messages = new HashMap<>();
  private Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();
  private Map<Integer, Map<Integer, Boolean>> finished = new HashMap<>();
  private Map<Integer, List<Object>> sendMessages = new HashMap<>();
  protected Map<Integer, Boolean> isEmptySent = new HashMap<>();
  private DataFlowOperation dataFlowOperation;
  private int executor;
  private int sendPendingMax = 128;
  private int destination;
  private Map<Integer, Boolean> batchDone = new HashMap<>();

  private int totalSendCount = 0;
  private int totalSendCountActual = 0;
  private Map<Integer, Integer> sourceCount = new HashMap<>();

  public KeyedGatherBatchPartialReceiver(int dst) {
    this.destination = dst;
  }

  @SuppressWarnings("unchecked")
  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    executor = op.getTaskPlan().getThisExecutor();
    sendPendingMax = DataFlowContext.sendPendingMax(cfg);

    LOG.fine(String.format("%d gather partial expected ids %s", executor, expectedIds));
    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      Map<Integer, Queue<Object>> messagesPerTask = new HashMap<>();
      Map<Integer, Boolean> finishedPerTask = new HashMap<>();
      Map<Integer, Integer> countsPerTask = new HashMap<>();

      for (int i : e.getValue()) {
        messagesPerTask.put(i, new ArrayBlockingQueue<>(sendPendingMax));
        finishedPerTask.put(i, false);
        countsPerTask.put(i, 0);
        sourceCount.put(i, 0);

      }
      messages.put(e.getKey(), messagesPerTask);
      finished.put(e.getKey(), finishedPerTask);
      isEmptySent.put(e.getKey(), false);
      sendMessages.put(e.getKey(), new ArrayList<Object>());
      counts.put(e.getKey(), countsPerTask);
      batchDone.put(e.getKey(), false);
    }
    this.dataFlowOperation = op;
    this.executor = dataFlowOperation.getTaskPlan().getThisExecutor();
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    // add the object to the map
    boolean canAdd = true;

    if (messages.get(target) == null) {
      throw new RuntimeException(String.format("%d Partial receive error %d", executor, target));
    }
    Map<Integer, Boolean> finishedMessages = finished.get(target);
    Queue<Object> m = messages.get(target).get(source);

    if ((flags & MessageFlags.END) == MessageFlags.END) {
      finishedMessages.put(source, true);
      return true;
    }


    if (m.size() >= sendPendingMax) {
      canAdd = false;
    } else {
      if (object instanceof ChannelMessage) {
        System.out.println(">>>>>>>>>>>>>>>>>>>>>>>>>>>>");
        ((ChannelMessage) object).incrementRefCount();
      }
      Integer c = counts.get(target).get(source);
      counts.get(target).put(source, c + 1);

      m.add(object);
      if (object instanceof List) {
        sourceCount.put(source, sourceCount.get(source) + ((List) object).size());
      } else {
        sourceCount.put(source, sourceCount.get(source) + 1);
      }
      if ((flags & MessageFlags.LAST) == MessageFlags.LAST) {
        finishedMessages.put(source, true);
      }
    }
    return canAdd;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    for (int target : messages.keySet()) {
      if (batchDone.get(target)) {
        if (!isEmptySent.get(target)) {
          if (dataFlowOperation.isDelegeteComplete() && dataFlowOperation.sendPartial(target,
              new byte[0], MessageFlags.END, destination)) {
            String output = "";
            for (Integer integer : sourceCount.keySet()) {
              output += " :: " + integer + "," + sourceCount.get(integer);
            }
            System.out.printf("%d Number of total sends to %d is %d actual %d :: %s ::::::: \n",
                executor,
                target, totalSendCount, totalSendCountActual, output);
            isEmptySent.put(target, true);
          } else {
            needsFurtherProgress = true;
          }
        }
        continue;
      }
      boolean canProgress = true;
      while (canProgress) {
        // now check weather we have the messages for this source
        Map<Integer, Queue<Object>> map = messages.get(target);
        Map<Integer, Boolean> finishedForTarget = finished.get(target);
        List<Object> sendList = sendMessages.get(target);

        boolean found = true;
        boolean allFinished = true;
        boolean moreThanOne = false;
        for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
          if (e.getValue().size() == 0 && !finishedForTarget.get(e.getKey())) {
            found = false;
            canProgress = false;
          } else if (e.getValue().size() > 0) {
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
        boolean allZero = true;

        if (found) {

          //If we have got all the last messages then even if we don't get from all expected id's
          //we can flush the remaining data
          if (allFinished && dataFlowOperation.isDelegeteComplete()) {
            for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
              Queue<Object> valueList = e.getValue();
              while (valueList.size() > 0) {
                Object value = valueList.poll();
                if (value instanceof List) {
                  sendList.addAll((List) value);
                  totalSendCount = totalSendCount + ((List) value).size();
                } else {
                  sendList.add(value);
                  totalSendCount++;
                }
              }
            }
          } else {
            for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
              Queue<Object> valueList = e.getValue();
              if (valueList.size() > 0) {
                Object value = valueList.poll();
                if (value instanceof List) {
                  sendList.addAll((List) value);
                  totalSendCount = totalSendCount + ((List) value).size();
                } else {
                  sendList.add(value);
                  totalSendCount++;
                }
              }
            }
          }

          int flags = 0;
          if (allFinished) {
            boolean last = true;
            for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
              Queue<Object> valueList = e.getValue();
              if (valueList.size() > 1) {
                last = false;
              }
            }
            if (last) {
              flags = MessageFlags.LAST;
            }
          }
          if (dataFlowOperation.sendPartial(target, sendList, flags, destination)) {
            totalSendCountActual += sendList.size();
            sendMessages.put(target, new ArrayList<Object>());
            for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
              Queue<Object> value = e.getValue();
              if (value.size() != 0) {
                allZero = false;
              }
            }


            if (allFinished && allZero) {
              batchDone.put(target, true);
              // we don't want to go through the while loop for this one
              break;
            }
          } else {
            canProgress = false;
            needsFurtherProgress = true;
          }
        }
        if (dataFlowOperation.isDelegeteComplete() && allFinished && allZero) {
          if (dataFlowOperation.sendPartial(target, new byte[0],
              MessageFlags.END, destination)) {
            String output = "";
            for (Integer integer : sourceCount.keySet()) {
              output += " :: " + integer + "," + sourceCount.get(integer);
            }
            System.out.printf("%d Number of total sends to %d is %d actual %d :: %s ::::::: \n",
                executor,
                target, totalSendCount, totalSendCountActual, output);
            isEmptySent.put(target, true);
          } else {
            needsFurtherProgress = true;
          }
          break;
        }
      }
    }
    return needsFurtherProgress;
  }
}

