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

import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.MessageFlags;
import edu.iu.dsc.tws.comms.api.ReduceFunction;

public class ReduceBatchPartialReceiver extends ReduceBatchReceiver {
  private static final Logger LOG = Logger.getLogger(ReduceBatchPartialReceiver.class.getName());

  public ReduceBatchPartialReceiver(int dst, ReduceFunction reduce) {
    super(dst, reduce);
    this.destination = dst;
    this.reduceFunction = reduce;
  }

  @Override
  public void progress() {
    for (int t : messages.keySet()) {
      if (batchDone.get(t)) {
        continue;
      }
      // now check weather we have the messages for this source
      Map<Integer, Queue<Object>> messagePerTarget = messages.get(t);
      Map<Integer, Boolean> finishedForTarget = finished.get(t);
      Map<Integer, Integer> countMap = counts.get(t);
      Map<Integer, Integer> totalCountMap = totalCounts.get(t);
      boolean canProgress = true;

      while (canProgress) {
//        LOG.info(String.format("%d reduce partial counts %d %s %s %s %d", executor, t,
//            countMap, totalCountMap, finishedForTarget, reducedValues.size()));
        boolean found = true;
        boolean allFinished = true;
        boolean allZero = true;

        for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
          if (e.getValue().size() == 0 && !finishedForTarget.get(e.getKey())) {
            found = false;
            canProgress = false;
          }
          if (!finishedForTarget.get(e.getKey())) {
            allFinished = false;
          }
        }

        if (found && reducedValues.size() < sendPendingMax) {
          Object previous = null;
          for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
            Queue<Object> valueList = e.getValue();
            if (valueList.size() > 0) {
              if (previous == null) {
                previous = valueList.poll();
              } else {
                Object current = valueList.poll();
                previous = reduceFunction.reduce(previous, current);
              }
            }
          }
          if (previous != null) {
            reducedValues.offer(previous);
          }
        }

        if (reducedValues.size() > 0) {
          Object previous = reducedValues.peek();
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
              flags = MessageFlags.FLAGS_LAST;
            }
          }

          if (dataFlowOperation.sendPartial(t, previous, flags, destination)) {
            // lets remove the value
            reducedValues.poll();

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
            if (allFinished && allZero && reducedValues.size() == 0) {
              batchDone.put(t, true);
              // we don't want to go through the while loop for this one
              break;
            }
          } else {
//          LOG.info(String.format("%d FALSE reduce partial counts %d %s %s", executor, t, countMap,
//                finishedForTarget));
            canProgress = false;
          }
        }
      }
    }
  }
}
