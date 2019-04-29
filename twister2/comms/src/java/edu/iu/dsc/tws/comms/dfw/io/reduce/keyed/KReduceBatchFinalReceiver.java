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

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.dfw.io.ReceiverState;
import edu.iu.dsc.tws.comms.dfw.io.TargetFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;

public class KReduceBatchFinalReceiver extends TargetFinalReceiver {
  /**
   * Final receiver that get the reduced values for the operation
   */
  private BulkReceiver bulkReceiver;

  /**
   * Reduce function
   */
  protected ReduceFunction reduceFunction;

  /**
   * The reduced values for each target and key
   */
  protected Map<Integer, Map<Object, Object>> reduced = new HashMap<>();

  public KReduceBatchFinalReceiver(ReduceFunction reduce, BulkReceiver receiver) {
    this.reduceFunction = reduce;
    this.bulkReceiver = receiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    this.bulkReceiver.init(cfg, expectedIds.keySet());
    for (int t : expectedIds.keySet()) {
      reduced.put(t, new HashMap<>());
    }
  }

  @Override
  protected void merge(int dest, Queue<Object> dests) {
    Map<Object, Object> targetValues = reduced.get(dest);

    while (dests.size() > 0) {
      Object val = dests.poll();
      Tuple t;

      if (val instanceof Tuple) {
        t = (Tuple) val;
      } else {
        throw new RuntimeException("Un-expected type: " + val.getClass());
      }

      Object currentVal = targetValues.get(t.getKey());
      if (currentVal != null) {
        Object newVal = reduceFunction.reduce(currentVal, t.getValue());
        targetValues.put(t.getKey(), newVal);
      } else {
        targetValues.put(t.getKey(), t.getValue());
      }
    }
  }

  @Override
  protected boolean isAllEmpty() {
    boolean b = super.isAllEmpty();
    return b && reduced.isEmpty();
  }

  @Override
  protected boolean sendToTarget(int source, int target) {
    Map<Object, Object> values = reduced.get(target);

    if (values == null || values.isEmpty()) {
      return true;
    }

    boolean send = bulkReceiver.receive(target, new ReduceIterator(values));
    if (send) {
      reduced.remove(target);
    }
    return send;
  }

  @Override
  protected boolean isFilledToSend(int target) {
    return targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED
        && messages.get(target).isEmpty();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private class ReduceIterator implements Iterator<Object> {
    private Iterator<Map.Entry<Object, Object>> it;

    ReduceIterator(Map<Object, Object> messageMap) {
      it = messageMap.entrySet().iterator();
    }

    @Override
    public boolean hasNext() {
      return it.hasNext();
    }

    @Override
    public Tuple next() {
      Map.Entry<Object, Object> entry = it.next();
      return new Tuple(entry.getKey(), entry.getValue(),
          operation.getKeyType(), operation.getDataType());
    }
  }

  @Override
  public void onSyncEvent(int target, byte[] value) {
    bulkReceiver.sync(target, value);
  }
}
