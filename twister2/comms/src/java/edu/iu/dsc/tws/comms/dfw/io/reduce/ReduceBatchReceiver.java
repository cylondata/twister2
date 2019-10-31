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

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.ReduceFunction;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.dfw.io.ReceiverState;
import edu.iu.dsc.tws.comms.dfw.io.SourceReceiver;

public abstract class ReduceBatchReceiver extends SourceReceiver {
  private static final Logger LOG = Logger.getLogger(ReduceBatchReceiver.class.getName());

  protected ReduceFunction reduceFunction;

  protected Map<Integer, Object> reducedValueMap = new HashMap<>();


  public ReduceBatchReceiver(ReduceFunction reduceFunction) {
    this.reduceFunction = reduceFunction;
  }

  public ReduceBatchReceiver(int dst, ReduceFunction reduce) {
    this.destination = dst;
    this.reduceFunction = reduce;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      reducedValueMap.put(e.getKey(), null);
    }
  }

  public abstract boolean handleMessage(int source, Object message, int flags, int dest);

  @Override
  protected boolean sendToTarget(int target, boolean sync) {
    Object reducedValue = this.reducedValueMap.get(target);
    if (reducedValue != null) {
      boolean handle = handleMessage(target, reducedValue, 0, destination);
      if (handle) {
        reducedValueMap.put(target, null);
      } else {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean aggregate(int target, boolean sync, boolean allValuesFound) {
    Map<Integer, Queue<Object>> messagePerTarget = messages.get(target);

    Object previous = reducedValueMap.get(target);
    for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
      if (previous == null) {
        previous = e.getValue().poll();
      } else {
        Object current = e.getValue().poll();
        if (current != null) {
          previous = reduceFunction.reduce(previous, current);
        }
      }
    }
    if (previous != null) {
      reducedValueMap.put(target, previous);
    }
    return true;
  }

  @Override
  protected boolean isAllEmpty(int target) {
    return reducedValueMap.get(target) == null;
  }

  @Override
  protected boolean isFilledToSend(int target, boolean sync) {
    if (targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED
        && allQueuesEmpty(messages.get(target))) {
      return reducedValueMap.get(target) != null;
    }
    return false;
  }

  @Override
  protected void onSyncEvent(int target, byte[] value) {

  }
}
