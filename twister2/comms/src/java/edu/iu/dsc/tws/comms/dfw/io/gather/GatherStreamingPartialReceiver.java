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
import java.util.TreeMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.dfw.ChannelMessage;
import edu.iu.dsc.tws.comms.dfw.DataFlowContext;

public class GatherStreamingPartialReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(
      GatherStreamingPartialReceiver.class.getName());
  // lets keep track of the messages
  // for each task we need to keep track of incoming messages
  private Map<Integer, Map<Integer, Queue<Object>>> messages = new TreeMap<>();
  private Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();
  private int currentIndex = 0;
  private DataFlowOperation dataFlowOperation;
  private int executor;
  private int sendPendingMax;

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    executor = op.getTaskPlan().getThisExecutor();
    this.sendPendingMax = DataFlowContext.sendPendingMax(cfg);

    LOG.info(String.format("%d expected ids %s", executor, expectedIds));
    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      Map<Integer, Queue<Object>> messagesPerTask = new HashMap<>();
      Map<Integer, Integer> countsPerTask = new HashMap<>();

      for (int i : e.getValue()) {
        messagesPerTask.put(i, new ArrayBlockingQueue<>(sendPendingMax));
        countsPerTask.put(i, 0);
      }

      messages.put(e.getKey(), messagesPerTask);
      counts.put(e.getKey(), countsPerTask);
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
    Queue<Object> m = messages.get(target).get(source);
    Integer c = counts.get(target).get(source);
    if (m.size() >= sendPendingMax) {
      canAdd = false;
//       LOG.info(String.format("%d Partial false: target %d source %d", executor, target, source));
    } else {
      // we need to increment the reference count to make the buffers available
      // other wise they will bre reclaimed
//        LOG.info(String.format("%d Partial true: target %d source %d %s",
//            executor, target, source, counts.get(target)));
      if (object instanceof ChannelMessage) {
        ((ChannelMessage) object).incrementRefCount();
      }
      m.add(object);
      counts.get(target).put(source, c + 1);
    }
    return canAdd;
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    for (int t : messages.keySet()) {
      boolean canProgress = true;
      while (canProgress) {
        // now check weather we have the messages for this source
        Map<Integer, Queue<Object>> map = messages.get(t);
        Map<Integer, Integer> cMap = counts.get(t);
        boolean found = true;
        boolean moreThanOne = false;
        for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
          if (e.getValue().size() == 0) {
            found = false;
            canProgress = false;
          } else {
            moreThanOne = true;
          }
        }

        // if we have queues with 0 and more than zero we need further progress
        if (!found && moreThanOne) {
          needsFurtherProgress = true;
        }

        if (map.entrySet().size() == 0) {
          LOG.info(String.format("%d entry size is ZERO %d %s", executor, t, counts));
        }

        if (found) {
          List<Object> out = new ArrayList<>();
          for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
            Object e1 = e.getValue().peek();
            if (e1 instanceof List) {
              out.addAll((List) e1);
            } else {
              out.add(e1);
            }
          }
          if (handleMessage(t, out, 0, t)) {
            for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
              Queue<Object> value = e.getValue();
              if (value.size() == 0) {
                LOG.info(String.format("%d list size ZERO task %d %d", executor, t, e.getKey()));
              }
              value.poll();
            }
            for (Map.Entry<Integer, Integer> e : cMap.entrySet()) {
              Integer i = e.getValue();
              e.setValue(i - 1);
            }
          } else {
            canProgress = false;
            needsFurtherProgress = true;
          }
        }
      }
    }
    return needsFurtherProgress;
  }

  protected boolean handleMessage(int task, Object message, int flags, int dest) {
    return dataFlowOperation.sendPartial(task, message, flags, dest);
  }
}
