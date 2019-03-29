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
package edu.iu.dsc.tws.comms.dfw.io.reduce;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.ReduceFunction;

public abstract class BaseReduceBatchFinalReceiver extends ReduceBatchReceiver {
  /**
   * Reduce function
    */
  protected ReduceFunction reduceFunction;

  /**
   * Final messages calculated from applying the reduce function
   */
  private Map<Integer, List<Object>> finalMessages = new HashMap<>();

  public BaseReduceBatchFinalReceiver(ReduceFunction reduce) {
    super(reduce);
    this.reduceFunction = reduce;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      finalMessages.put(e.getKey(), new ArrayList<>());
    }
  }

  /**
   * Method used to communicationProgress work
   */
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
        }
      }

      // if we have queues with 0 and more than zero we need further communicationProgress
      if (!found && moreThanOne) {
        needsFurtherProgress = true;
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

      if (!dataFlowOperation.isDelegateComplete()) {
        allFinished = false;
        needsFurtherProgress = true;
      }

      if (allFinished) {
        Object previous = null;
        List<Object> finalMessagePerTask = finalMessages.get(t);
        for (Object aFinalMessagePerTask : finalMessagePerTask) {
          if (previous == null) {
            previous = aFinalMessagePerTask;
          } else {
            previous = reduceFunction.reduce(previous, aFinalMessagePerTask);
          }
        }

        if (handleFinished(t, previous)) {
          batchDone.put(t, true);
          onFinish(t);
        } else {
          needsFurtherProgress = true;
        }
      } else {
        needsFurtherProgress = true;
      }
    }
    return needsFurtherProgress;
  }

  /**
   * Handle finish
   * @param task task
   * @param value value
   * @return true if success
   */
  protected abstract boolean handleFinished(int task, Object value);

  @Override
  public void onFinish(int source) {
    super.onFinish(source);
  }
}
