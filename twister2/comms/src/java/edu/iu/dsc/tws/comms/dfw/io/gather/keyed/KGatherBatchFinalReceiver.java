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

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import edu.iu.dsc.tws.api.comms.BulkReceiver;
import edu.iu.dsc.tws.api.comms.DataFlowOperation;
import edu.iu.dsc.tws.api.comms.structs.Tuple;
import edu.iu.dsc.tws.api.config.Config;
import edu.iu.dsc.tws.comms.dfw.io.AggregatedObjects;
import edu.iu.dsc.tws.comms.dfw.io.ReceiverState;
import edu.iu.dsc.tws.comms.dfw.io.TargetFinalReceiver;

import edu.iu.dsc.tws.comms.utils.THashMap;

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;

/**
 * Final receiver for keyed gather
 */
public class KGatherBatchFinalReceiver extends TargetFinalReceiver {
  /**
   * Final receiver that get the reduced values for the operation
   */
  private BulkReceiver bulkReceiver;
  private boolean groupByKey;

  /**
   * The reduced values for each target and key
   */
  protected Int2ObjectOpenHashMap<Map<Object, List<Object>>> gathered =
      new Int2ObjectOpenHashMap<>();

  public KGatherBatchFinalReceiver(BulkReceiver receiver,
                                   boolean groupByKey) {
    this.bulkReceiver = receiver;
    this.groupByKey = groupByKey;
  }

  @Override
  public void init(Config cfg, DataFlowOperation op, Map<Integer, List<Integer>> expectedIds) {
    super.init(cfg, op, expectedIds);
    this.bulkReceiver.init(cfg, expectedIds.keySet());
    for (int t : expectedIds.keySet()) {
      gathered.put(t, new THashMap<>());
    }
  }

  @Override
  protected void merge(int dest, List<Object> dests) {
    Map<Object, List<Object>> targetValues = gathered.get(dest);

    for (int i = 0; i < dests.size(); i++) {
      Object val = dests.get(i);
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
    dests.clear();
  }

  @Override
  protected boolean sendToTarget(int source, int target) {
    Map<Object, List<Object>> values = gathered.get(target);

    if (values == null || values.isEmpty()) {
      return true;
    }

    boolean send = bulkReceiver.receive(target,
        this.groupByKey ? new GroupedGatherIterator(values) : new UnGroupedGatherIterator(values));
    if (send) {
      gathered.remove(target);
    }
    return send;
  }

  @Override
  protected boolean isAllEmpty(int target) {
    if (gathered.containsKey(target)) {
      Map<Object, List<Object>> queue = gathered.get(target);
      return queue.isEmpty();
    }
    return true;
  }

  @Override
  protected boolean isFilledToSend(int target) {
    return targetStates.get(target) == ReceiverState.ALL_SYNCS_RECEIVED
        && messages.get(target).isEmpty();
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private class UnGroupedGatherIterator implements Iterator<Object> {

    private Map<Object, List<Object>> messageMap;
    private Queue<Object> keyList = new LinkedList<>();

    private Object currentKey;
    private List<Object> currentValues;
    private int currentIndex = 0;

    UnGroupedGatherIterator(Map<Object, List<Object>> messageMap) {
      this.messageMap = messageMap;
      keyList.addAll(messageMap.keySet());
      this.moveToNextKey();
    }

    private void moveToNextKey() {
      this.currentKey = this.keyList.poll();
      this.currentValues = this.messageMap.getOrDefault(this.currentKey,
          Collections.emptyList());
      this.currentIndex = 0;
    }

    @Override
    public boolean hasNext() {
      return currentKey != null;
    }

    @Override
    public Object next() {
      Tuple tuple;
      if (this.currentKey instanceof ByteBuffer) {
        tuple = new Tuple(((ByteBuffer) this.currentKey).array(),
            currentValues.get(currentIndex++));
      } else {
        tuple = new Tuple(this.currentKey, currentValues.get(currentIndex++));
      }


      if (this.currentIndex == this.currentValues.size()) {
        this.moveToNextKey();
      }

      return tuple;
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private class GroupedGatherIterator implements Iterator<Object> {

    private Map<Object, List<Object>> messageMap;
    private Queue<Object> keyList = new LinkedList<>();

    GroupedGatherIterator(Map<Object, List<Object>> messageMap) {
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
      if (key instanceof ByteBuffer) {
        return new Tuple(((ByteBuffer) key).array(), value.iterator());
      }
      return new Tuple(key, value.iterator());
    }
  }

  @Override
  public boolean onSyncEvent(int target, byte[] value) {
    return bulkReceiver.sync(target, value);
  }
}
