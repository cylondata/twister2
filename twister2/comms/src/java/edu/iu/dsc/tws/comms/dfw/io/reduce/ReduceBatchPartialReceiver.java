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

import java.util.Map;
import java.util.Queue;

import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.ReduceFunction;

public class ReduceBatchPartialReceiver extends ReduceBatchReceiver {

  public ReduceBatchPartialReceiver(int dst, ReduceFunction reduce) {
    super(dst, reduce);
    this.destination = dst;
    this.reduceFunction = reduce;
  }

  @Override
  public boolean progress() {
    boolean needsFurtherProgress = false;
    for (int t : messages.keySet()) {
      if (batchDone.get(t)) {
        if (!isEmptySent.get(t)) {
          if (dataFlowOperation.isDelegateComplete() && dataFlowOperation.sendPartial(t,
              new byte[0], MessageFlags.END, destination)) {
            isEmptySent.put(t, true);
          } else {
            needsFurtherProgress = true;
          }
        }
        continue;
      }
      // now check weather we have the messages for this source
      Map<Integer, Queue<Object>> messagePerTarget = messages.get(t);
      Map<Integer, Boolean> finishedForTarget = finished.get(t);
      Map<Integer, Integer> countMap = counts.get(t);

      boolean canProgress = true;
      int tempBufferCount = bufferCounts.get(t);
      Object currentVal = null;

      while (canProgress) {
        boolean found = true;
        boolean allFinished = true;
        boolean allZero = true;

        boolean moreThanOne = false;
        for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
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

        // if we have queues with 0 and more than zero we need further communicationProgress
        if (!found && moreThanOne) {
          needsFurtherProgress = true;
        }
        if (found) {
          currentVal = reducedValueMap.get(t);
          for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
            Queue<Object> valueList = e.getValue();
            if (valueList.size() > 0) {
              if (currentVal == null) {
                currentVal = valueList.poll();
                tempBufferCount += 1;
              } else {
                Object current = valueList.poll();
                currentVal = reduceFunction.reduce(currentVal, current);
                tempBufferCount += 1;
              }
            }
          }
          bufferCounts.put(t, tempBufferCount);
          if (currentVal != null) {
            reducedValueMap.put(t, currentVal);
          }
        }

        if ((!bufferTillEnd && tempBufferCount >= bufferSize) || allFinished) {
          int flags = 0;
          boolean last;
          if (allFinished) {
            last = true;
            for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
              Queue<Object> valueList = e.getValue();
              if (valueList.size() > 1) {
                last = false;
              }
            }
            if (last) {
              flags = MessageFlags.LAST;
            }
          }

          if (currentVal != null
              && dataFlowOperation.sendPartial(t, currentVal, flags, destination)) {
            // lets remove the value
            bufferCounts.put(t, 0);
            tempBufferCount = 0;
            reducedValueMap.put(t, null);
            for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
              Queue<Object> value = e.getValue();
              if (value.size() != 0) {
                allZero = false;
              }
            }

            for (Map.Entry<Integer, Integer> e : countMap.entrySet()) {
              Integer i = e.getValue();
              e.setValue(i - 1);
            }

          } else {
            canProgress = false;
            needsFurtherProgress = true;
          }

          if (dataFlowOperation.isDelegateComplete() && allFinished && allZero) {
            if (dataFlowOperation.sendPartial(t, new byte[0],
                MessageFlags.END, destination)) {
              isEmptySent.put(t, true);
            } else {
              needsFurtherProgress = true;
            }
            batchDone.put(t, true);
            // we don't want to go through the while loop for this one
            break;
          } else {
            needsFurtherProgress = true;
          }
        }
      }
    }
    return needsFurtherProgress;
  }

  @Override
  public void onFinish(int source) {
    super.onFinish(source);
  }
}
