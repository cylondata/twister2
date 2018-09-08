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
package edu.iu.dsc.tws.comms.dfw.io.reduce.keyed;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.KeyedReduceFunction;
import edu.iu.dsc.tws.comms.api.ReduceReceiver;
import edu.iu.dsc.tws.comms.dfw.io.reduce.ReduceBatchReceiver;

public class KeyedReduceBatchFinalReceiver extends ReduceBatchReceiver {
  private static final Logger LOG = Logger.getLogger(KeyedReduceBatchFinalReceiver.class.getName());

  private KeyedReduceFunction reduceFunction;

  private ReduceReceiver reduceReceiver;

  private Map<Integer, List<Object>> finalMessages = new HashMap<>();

  public KeyedReduceBatchFinalReceiver(KeyedReduceFunction reduce, ReduceReceiver receiver) {
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
   * Method used to communicationProgress work
   */
  public boolean progress() {
    boolean needsFurtherProgress = false;
    for (int target : messages.keySet()) {
      if (batchDone.get(target)) {
        continue;
      }
      boolean allFinished = true;
      // now check weather we have the messages for this source
      Map<Integer, Queue<Object>> messagesForTarget = messages.get(target);
      Map<Integer, Boolean> finishedForTarget = finished.get(target);
      Map<Integer, Integer> countMap = counts.get(target);
      Map<Integer, Integer> totalCountMap = totalCounts.get(target);
      Set<Integer> emptyMessages = emptyReceivedSources.get(target);

      boolean found = true;

      boolean moreThanOne = false;
      for (Map.Entry<Integer, Queue<Object>> targetSourceEntry : messagesForTarget.entrySet()) {
        if (targetSourceEntry.getValue().size() == 0
            && !finishedForTarget.get(targetSourceEntry.getKey())) {
          found = false;
        } else if (targetSourceEntry.getValue().size() > 0) {
          moreThanOne = true;
        }

        if (!finishedForTarget.get(targetSourceEntry.getKey())) {
          allFinished = false;
        }
      }

      // if we have queues with 0 and more than zero we need further communicationProgress
      if (!found && moreThanOne) {
        needsFurtherProgress = true;
      }

      if (found) {
        List<Object> out = new ArrayList<>();
        for (Map.Entry<Integer, Queue<Object>> e : messagesForTarget.entrySet()) {
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
        finalMessages.get(target).addAll(out);
      } else {
        allFinished = false;
      }

      if (!dataFlowOperation.isDelegeteComplete()) {
        allFinished = false;
        needsFurtherProgress = true;
      }

      if (allFinished) {
        batchDone.put(target, true);
        Object previous = null;
        List<Object> finalMessagePerTask = finalMessages.get(target);
        for (int i = 0; i < finalMessagePerTask.size(); i++) {
          if (previous == null) {
            previous = finalMessagePerTask.get(i);
          } else {
            Object current = finalMessagePerTask.get(i);
            previous = reduceFunction.reduce(previous, current);
          }

        }
        reduceReceiver.receive(target, previous);
      }
    }
    return needsFurtherProgress;
  }

  @Override
  public void onFinish(int source) {
    super.onFinish(source);
  }
}
