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
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.dfw.io.AggregatedObjects;
import edu.iu.dsc.tws.comms.dfw.io.SourceReceiver;

public abstract class BaseGatherBatchReceiver extends SourceReceiver {
  protected static final Logger LOG = Logger.getLogger(BaseGatherBatchReceiver.class.getName());

  protected Map<Integer, List<Object>> gatheredValuesMap = new HashMap<>();

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);

    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      gatheredValuesMap.put(e.getKey(), new AggregatedObjects<>(sendPendingMax));
    }
  }

  @Override
  protected boolean sendToTarget(int target, boolean sync) {
    List<Object> gatheredValues = this.gatheredValuesMap.get(target);
    if (gatheredValues != null) {
      boolean handle = handleMessage(target, gatheredValues, 0, destination);
      if (handle) {
        gatheredValuesMap.put(target, null);
      } else {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean aggregate(int target, boolean sync, boolean allValuesFound) {
    List<Object> reducedValues = this.gatheredValuesMap.get(target);
    Map<Integer, Queue<Object>> messagePerTarget = messages.get(target);

    List<Object> out = new AggregatedObjects<>();
    for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
      Object value = e.getValue().poll();

      if (value == null) {
        continue;
      }

      if (value instanceof AggregatedObjects) {
        out.addAll((List) value);
      } else {
        out.add(value);
      }
    }
    if (out.size() > 0) {
      if (reducedValues == null) {
        reducedValues = new AggregatedObjects<>();
        gatheredValuesMap.put(target, reducedValues);
      }
      reducedValues.addAll(out);
    }
    return true;
  }

  @Override
  protected boolean isAllEmpty(int target) {
    return gatheredValuesMap.get(target) == null || gatheredValuesMap.get(target).isEmpty();
  }

  protected abstract boolean handleMessage(int task, Object message, int flags, int dest);

  @Override
  protected void onSyncEvent(int target, byte[] value) {
  }
}
