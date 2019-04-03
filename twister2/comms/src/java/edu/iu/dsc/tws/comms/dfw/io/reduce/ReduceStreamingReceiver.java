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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.dfw.io.SourceReceiver;

public abstract class ReduceStreamingReceiver extends SourceReceiver {
  private static final Logger LOG = Logger.getLogger(ReduceStreamingReceiver.class.getName());

  protected ReduceFunction reduceFunction;
  private Map<Integer, Queue<Object>> reducedValuesMap = new HashMap<>();

  public ReduceStreamingReceiver(ReduceFunction function) {
    this(0, function);
  }

  public ReduceStreamingReceiver(int dst, ReduceFunction function) {
    this.reduceFunction = function;
    this.destination = dst;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);

    for (Map.Entry<Integer, List<Integer>> e : expectedIds.entrySet()) {
      reducedValuesMap.put(e.getKey(), new ArrayBlockingQueue<>(sendPendingMax));
    }
  }

  @Override
  protected boolean sendToTarget(int target) {
    Queue<Object> reducedValues = this.reducedValuesMap.get(target);
    while (reducedValues.size() > 0) {
      Object previous = reducedValues.peek();
      boolean handle = handleMessage(target, previous, 0, destination);
      if (handle) {
        reducedValues.poll();
      } else {
        return false;
      }
    }
    return true;
  }

  @Override
  protected boolean aggregate(int target) {
    Queue<Object> reducedValues = this.reducedValuesMap.get(target);
    Map<Integer, Queue<Object>> messagePerTarget = messages.get(target);

    if (reducedValues.size() < sendPendingMax) {
      Object previous = null;
      for (Map.Entry<Integer, Queue<Object>> e : messagePerTarget.entrySet()) {
        if (previous == null) {
          previous = e.getValue().poll();
        } else {
          Object current = e.getValue().poll();
          previous = reduceFunction.reduce(previous, current);
        }
      }
      if (previous != null) {
        reducedValues.offer(previous);
      }
      return true;
    } else {
      return false;
    }
  }

  public abstract boolean handleMessage(int source, Object message, int flags, int dest);
}
