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
package edu.iu.dsc.tws.comms.dfw.io.gather.keyed;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import edu.iu.dsc.tws.common.config.Config;
import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.DataFlowOperation;
import edu.iu.dsc.tws.comms.dfw.io.AggregatedObjects;
import edu.iu.dsc.tws.comms.dfw.io.ReceiverState;
import edu.iu.dsc.tws.comms.dfw.io.TargetFinalReceiver;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;

/**
 * Final receiver for keyed gather
 */
public class KGatherBatchFinalReceiver extends TargetFinalReceiver {
  /**
   * Final receiver that get the reduced values for the operation
   */
  private BulkReceiver bulkReceiver;

  /**
   * The reduced values for each target and key
   */
  protected Map<Integer, Map<Object, List<Object>>> gathered = new HashMap<>();

  public KGatherBatchFinalReceiver(BulkReceiver receiver,
                                   int limitPerKey) {
    this.bulkReceiver = receiver;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    this.bulkReceiver.init(cfg, expectedIds.keySet());
    for (int t : expectedIds.keySet()) {
      gathered.put(t, new HashMap<>());
    }
  }

  @Override
  protected void merge(int dest, Queue<Object> dests) {
    Map<Object, List<Object>> targetValues = gathered.get(dest);

    while (dests.size() > 0) {
      Object val = dests.poll();
      Tuple t;

      if (val instanceof Tuple) {
        t = (Tuple) val;
      } else {
        throw new RuntimeException("Un-expected type: " + val.getClass());
      }

      List<Object> currentVal = targetValues.get(t.getKey());
      if (currentVal == null) {
        currentVal = new AggregatedObjects<>();
        targetValues.put(t.getKey(), currentVal);
      }
      currentVal.add(t.getValue());
    }
  }

  @Override
  protected boolean sendToTarget(int source, int target) {
    Map<Object, List<Object>> values = gathered.get(target);

    if (values == null || values.isEmpty()) {
      return true;
    }

    boolean send = bulkReceiver.receive(target, new GatherIterator(values));
    if (send) {
      gathered.remove(target);
    }
    return send;
  }

  @Override
  protected boolean isAllEmpty() {
    boolean b = super.isAllEmpty();
    return b && gathered.isEmpty();
  }

  @Override
  protected boolean isFilledToSend(int target) {
    return targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED
        && messages.get(target).isEmpty();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private class GatherIterator implements Iterator<Object> {

    private Map<Object, List<Object>> messageMap;
    private Queue<Object> keyList = new LinkedList<>();

    GatherIterator(Map<Object, List<Object>> messageMap) {
      this.messageMap = messageMap;
      keyList.addAll(messageMap.keySet());
    }

    @Override
    public boolean hasNext() {
      return !keyList.isEmpty();
    }

    @Override
    public Object next() {
      Object key = keyList.poll();
      List value = messageMap.remove(key);
      return new Tuple(key, value.iterator(),
          operation.getKeyType(), operation.getDataType());
    }
  }

  @Override
  public void onSyncEvent(int target, byte[] value) {
    bulkReceiver.sync(target, value);
  }
}
