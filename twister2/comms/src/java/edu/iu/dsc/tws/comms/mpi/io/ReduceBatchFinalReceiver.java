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
package edu.iu.dsc.tws.comms.mpi.io;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.MessageReceiver;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.ReduceReceiver;
import edu.iu.dsc.tws.comms.mpi.MPIContext;
import edu.iu.dsc.tws.comms.mpi.MPIMessage;

public class ReduceBatchFinalReceiver implements MessageReceiver {
  private static final Logger LOG = Logger.getLogger(ReduceBatchFinalReceiver.class.getName());

  private ReduceFunction reduceFunction;

  private ReduceReceiver reduceReceiver;

  // lets keep track of the messages
  // for each task we need to keep track of incoming messages
  private Map<Integer, Map<Integer, List<Object>>> messages = new HashMap<>();
  private Map<Integer, Map<Integer, Boolean>> finished = new HashMap<>();
  private Map<Integer, List<Object>> finalMessages = new HashMap<>();
  private Map<Integer, Map<Integer, Integer>> counts = new HashMap<>();
  private DataFlowOperation dataFlowOperation;
  private int executor;
  private int sendPendingMax = 128;
  private Map<Integer, Boolean> batchDone = new HashMap<>();
  private Map<Integer, Map<Integer, Integer>> totalCounts = new HashMap<>();

  public ReduceBatchFinalReceiver(ReduceFunction reduce, ReduceReceiver receiver) {
    this.reduceFunction = reduce;
    this.reduceReceiver = receiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    executor = op.getTaskPlan().getThisExecutor();
    sendPendingMax = MPIContext.sendPendingMax(cfg);

//    LOG.fine(String.format("%d expected ids %s", executor, expectedIds));
    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      Map<Integer, List<Object>> messagesPerTask = new HashMap<>();
      Map<Integer, Boolean> finishedPerTask = new HashMap<>();
      Map<Integer, Integer> countsPerTask = new HashMap<>();
      Map<Integer, Integer> totalCountsPerTask = new HashMap<>();

      for (int i : e.getValue()) {
        messagesPerTask.put(i, new ArrayList<Object>());
        finishedPerTask.put(i, false);
        countsPerTask.put(i, 0);
        totalCountsPerTask.put(i, 0);
      }
      messages.put(e.getKey(), messagesPerTask);
      finished.put(e.getKey(), finishedPerTask);
      finalMessages.put(e.getKey(), new ArrayList<>());
      counts.put(e.getKey(), countsPerTask);
      batchDone.put(e.getKey(), false);
      totalCounts.put(e.getKey(), totalCountsPerTask);
    }
    this.dataFlowOperation = op;
    this.executor = dataFlowOperation.getTaskPlan().getThisExecutor();
  }

  @Override
  public boolean onMessage(int source, int path, int target, int flags, Object object) {
    // add the object to the map
    boolean canAdd = true;

    List<Object> m = messages.get(target).get(source);
    Map<Integer, Boolean> finishedMessages = finished.get(target);
    if (m.size() > sendPendingMax) {
      canAdd = false;
//      LOG.info(String.format("%d Final add FALSE target %d source %d %s %d",
//          executor, target, source, counts.get(target), finalMessages.size()));
    } else {
//      LOG.info(String.format("%d Final add TRUE target %d source %d %d",
//          executor, target, source, finishedMessages.size()));
      if (object instanceof MPIMessage) {
        ((MPIMessage) object).incrementRefCount();
      }

      Integer c = counts.get(target).get(source);
      counts.get(target).put(source, c + 1);

      Integer tc = totalCounts.get(target).get(source);
      totalCounts.get(target).put(source, tc + 1);

      m.add(object);
      if ((flags & MessageFlags.FLAGS_LAST) == MessageFlags.FLAGS_LAST) {
//        LOG.info(String.format("%d Final LAST target %d source %d", executor, target, source));
        finishedMessages.put(source, true);
      }
    }
    return canAdd;
  }

  /**
   * Method used to progress work
   */
  public void progress() {
    for (int t : messages.keySet()) {
      if (batchDone.get(t)) {
        continue;
      }

      boolean allFinished = true;
      // now check weather we have the messages for this source
      Map<Integer, List<Object>> map = messages.get(t);
      Map<Integer, Boolean> finishedForTarget = finished.get(t);
      Map<Integer, Integer> countMap = counts.get(t);
      Map<Integer, Integer> totalCountMap = totalCounts.get(t);
//      LOG.info(String.format("%d reduce final counts %d %s %s %s", executor, t, countMap,
//          totalCountMap, finishedForTarget));
      boolean found = true;
      for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
        if (e.getValue().size() == 0 && !finishedForTarget.get(e.getKey())) {
          found = false;
        }

        if (!finishedForTarget.get(e.getKey())) {
          allFinished = false;
        }
      }

      if (found) {
        List<Object> out = new ArrayList<>();
        for (Map.Entry<Integer, List<Object>> e : map.entrySet()) {
          List<Object> valueList = e.getValue();
          if (valueList.size() > 0) {
            Object value = valueList.get(0);
            out.add(value);
            allFinished = false;
            valueList.remove(0);
          }
        }
        for (Map.Entry<Integer, Integer> e : countMap.entrySet()) {
          Integer i = e.getValue();
          e.setValue(i - 1);
        }
        finalMessages.get(t).addAll(out);
      } else {
        allFinished = false;
      }

      if (allFinished) {
//        LOG.info(String.format("%d final all finished %d", executor, t));
        batchDone.put(t, true);
        Object previous = null;
        List<Object> finalMessagePerTask = finalMessages.get(t);
        for (int i = 0; i < finalMessagePerTask.size(); i++) {
          if (previous == null) {
            previous = finalMessagePerTask.get(i);
          } else {
            Object current = finalMessagePerTask.get(i);
            previous = reduceFunction.reduce(previous, current);
          }
        }
        reduceReceiver.receive(t, previous);
      }
    }
  }
}
