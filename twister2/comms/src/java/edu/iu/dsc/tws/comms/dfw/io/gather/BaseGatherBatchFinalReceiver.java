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

public abstract class BaseGatherBatchFinalReceiver extends BaseGatherBatchReceiver {
  /**
   * Final messages gathered
   */
  protected Map<Integer, List<Object>> finalMessages = new HashMap<>();
  /**
   * Weather we have sent the outputs for this target
   */
  protected Map<Integer, Boolean> batchDone = new HashMap<>();

  @Override
  protected void init() {
    for (Map.Entry<Integer, List<Integer>> e : expIds.entrySet()) {
      finalMessages.put(e.getKey(), new ArrayList<>());
      batchDone.put(e.getKey(), false);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
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
        handleFinish(t);
      }
    }
    return needsFurtherProgress;
  }

  protected abstract void handleFinish(int t);
}
