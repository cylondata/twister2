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
package edu.iu.dsc.tws.comms.mpi.io.reduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.api.ReduceReceiver;

public class ReduceBatchFinalReceiver extends ReduceBatchReceiver {
  private static final Logger LOG = Logger.getLogger(ReduceBatchFinalReceiver.class.getName());

  private ReduceFunction reduceFunction;

  private ReduceReceiver reduceReceiver;

  private Map<Integer, List<Object>> finalMessages = new HashMap<>();

  public ReduceBatchFinalReceiver(ReduceFunction reduce, ReduceReceiver receiver) {
    super(reduce);
    this.reduceFunction = reduce;
    this.reduceReceiver = receiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    reduceReceiver.init(cfg, op, expectedIds);
    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      finalMessages.put(e.getKey(), new ArrayList<>());
    }
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
      Map<Integer, Queue<Object>> map = messages.get(t);
      Map<Integer, Boolean> finishedForTarget = finished.get(t);
      Map<Integer, Integer> countMap = counts.get(t);
      Map<Integer, Integer> totalCountMap = totalCounts.get(t);
//      LOG.info(String.format("%d reduce final counts %d %s %s %s", executor, t, countMap,
//          totalCountMap, finishedForTarget));
      boolean found = true;
      for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
        if (e.getValue().size() == 0 && !finishedForTarget.get(e.getKey())) {
          found = false;
        }

        if (!finishedForTarget.get(e.getKey())) {
          allFinished = false;
        }
      }

      if (found) {
        List<Object> out = new ArrayList<>();
        for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
          Queue<Object> valueList = e.getValue();
          if (valueList.size() > 0) {
            Object value = valueList.poll();
            out.add(value);
            allFinished = false;
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
