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

import edu.iu.dsc.tws.comms.api.MessageFlags;

public class GatherBatchPartialReceiver extends BaseGatherBatchReceiver {
  private Map<Integer, Boolean> isEmptySent = new HashMap<>();
  private int destination;
  private Map<Integer, Boolean> batchDone = new HashMap<>();

  public GatherBatchPartialReceiver(int dst) {
    this.destination = dst;
  }

  @Override
  protected void init() {
    for (Map.Entry<Integer, List<Integer>> e : expIds.entrySet()) {
      isEmptySent.put(e.getKey(), false);
      batchDone.put(e.getKey(), false);
    }
  }

  @SuppressWarnings("unchecked")
  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    for (int target : messages.keySet()) {
      if (batchDone.get(target)) {
        if (!isEmptySent.get(target)) {
          if (dataFlowOperation.isDelegateComplete() && dataFlowOperation.sendPartial(target,
              new byte[0], MessageFlags.END, destination)) {
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
        Map<Integer, Integer> countMap = counts.get(target);
        boolean found = true;
        boolean allFinished = true;
        boolean moreThanOne = false;
        for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
          if (e.getValue().size() == 0 && !finishedForTarget.get(e.getKey())) {
            found = false;
            canProgress = false;
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
        boolean allZero = true;

        if (found) {
          List<Object> out = new ArrayList<>();
          for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
            Queue<Object> valueList = e.getValue();
            if (valueList.size() > 0) {
              Object value = valueList.peek();
              if (value instanceof List) {
                out.addAll((List) value);
              } else {
                out.add(value);
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
          if (dataFlowOperation.sendPartial(target, out, flags, destination)) {
            for (Map.Entry<Integer, Queue<Object>> e : map.entrySet()) {
              Queue<Object> value = e.getValue();
              if (value.size() > 0) {
                value.poll();
              }
              if (value.size() != 0) {
                allZero = false;
              }
            }
            for (Map.Entry<Integer, Integer> e : countMap.entrySet()) {
              Integer i = e.getValue();
              e.setValue(i - 1);
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
        if (dataFlowOperation.isDelegateComplete() && allFinished && allZero) {
          if (dataFlowOperation.sendPartial(target, new byte[0],
              MessageFlags.END, destination)) {
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

