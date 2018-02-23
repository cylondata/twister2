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
package edu.iu.dsc.tws.comms.mpi.io.gather;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.mpi.MPIMessage;

public class StreamingPartialGatherReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(
      StreamingPartialGatherReceiver.class.getName());
  // lets keep track of the messages
  // for each task we need to keep track of incoming messages
  private Map<Integer, Map<Integer, List<Object>>> messages = new TreeMap<>();
  private Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();
  private int currentIndex = 0;
  private DataFlowOperation dataFlowOperation;
  private int executor;
  private String threadName;

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    executor = op.getTaskPlan().getThisExecutor();

    LOG.info(String.format("%d expected ids %s", executor, expectedIds));
    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      Map<Integer, List<Object>> messagesPerTask = new HashMap<>();
      Map<Integer, Integer> countsPerTask = new HashMap<>();

      for (int i : e.getValue()) {
        messagesPerTask.put(i, new ArrayList<Object>());
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

    if (this.threadName == null) {
      this.threadName = Thread.currentThread().getName();
    }
    String tn = Thread.currentThread().getName();
    if (!tn.equals(threadName)) {
      throw new RuntimeException(String.format("%d Threads are not equal %s %s",
          executor, threadName, tn));
    }
    if (messages.get(target) == null) {
      throw new RuntimeException(String.format("%d Partial receive error %d", executor, target));
    }
    List<Object> m = messages.get(target).get(source);
    Integer c = counts.get(target).get(source);
    if (m.size() > 16) {
      canAdd = false;
//       LOG.info(String.format("%d Partial false: target %d source %d", executor, target, source));
    } else {
      // we need to increment the reference count to make the buffers available
      // other wise they will bre reclaimed
//        LOG.info(String.format("%d Partial true: target %d source %d %s",
//            executor, target, source, counts.get(target)));
      if (object instanceof MPIMessage) {
        ((MPIMessage) object).incrementRefCount();
      }
      m.add(object);
      counts.get(target).put(source, c + 1);
    }
    return canAdd;
  }

  @Override
  public void progress() {
    if (this.threadName == null) {
      this.threadName = Thread.currentThread().getName();
    }
    String tn = Thread.currentThread().getName();
    if (!tn.equals(threadName)) {
      throw new RuntimeException(String.format("%d Threads are not equal %s %s",
          executor, threadName, tn));
    }

    for (int t : messages.keySet()) {
      boolean canProgress = true;
      while (canProgress) {
        // now check weather we have the messages for this source
        Map<Integer, List<Object>> map = messages.get(t);
        Map<Integer, Integer> cMap = counts.get(t);
        boolean found = true;
        for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
          if (e.getValue().size() == 0) {
            found = false;
            canProgress = false;
          }
        }

        if (map.entrySet().size() == 0) {
          LOG.info(String.format("%d entry size is ZERO %d %s", executor, t, counts));
        }

        if (found) {
          List<Object> out = new ArrayList<>();
          for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
            Object e1 = e.getValue().get(0);
            out.add(e1);
          }
          if (handleMessage(t, out, 0, t)) {
            for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
              List<Object> value = e.getValue();
              if (value.size() == 0) {
                LOG.info(String.format("%d list size ZERO task %d %d", executor, t, e.getKey()));
              }
              value.remove(0);
            }
            for (Map.Entry<Integer, Integer> e : cMap.entrySet()) {
              Integer i = e.getValue();
              e.setValue(i - 1);
            }
          } else {
            canProgress = false;
          }
        }
      }
    }
  }

  protected boolean handleMessage(int task, Object message, int flags, int dest) {
    return dataFlowOperation.sendPartial(task, message, flags, dest);
  }
}
