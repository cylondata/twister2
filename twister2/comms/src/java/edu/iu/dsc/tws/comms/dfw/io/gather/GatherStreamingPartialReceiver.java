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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.dfw.io.AggregatedObjects;
import edu.iu.dsc.tws.comms.dfw.io.SourceReceiver;

public class GatherStreamingPartialReceiver extends SourceReceiver {
  private Map<Integer, Queue<Object>> gatheredValuesMap = new HashMap<>();

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);

    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      gatheredValuesMap.put(e.getKey(), new ArrayBlockingQueue<>(sendPendingMax));
    }
  }

  @Override
  protected boolean sendToTarget(int target) {
    Queue<Object> gatheredValues = this.gatheredValuesMap.get(target);
    while (gatheredValues.size() > 0) {
      Object previous = gatheredValues.peek();
      boolean handle = handleMessage(target, previous, 0, destination);
      if (handle) {
        gatheredValues.poll();
      } else {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean aggregate(int target) {
    Queue<Object> reducedValues = this.gatheredValuesMap.get(target);
    Map<Integer, Queue<Object>> messagePerTarget = messages.get(target);

    if (reducedValues.size() < sendPendingMax) {
      List<Object> out = new AggregatedObjects<>();
      for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
        Object value = e.getValue().poll();
        if (value instanceof AggregatedObjects) {
          out.addAll((List) value);
        } else {
          out.add(value);
        }
      }
      gatheredValuesMap.get(target).add(out);
      return true;
    } else {
      return false;
    }
  }

  protected boolean handleMessage(int task, Object message, int flags, int dest) {
    return operation.sendPartial(task, message, flags, dest);
  }
}
