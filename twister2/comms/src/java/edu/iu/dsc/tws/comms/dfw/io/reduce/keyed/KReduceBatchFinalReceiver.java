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

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.logging.Logger;

import edu.iu.dsc.tws.comms.api.BulkReceiver;
import edu.iu.dsc.tws.comms.api.ReduceFunction;
import edu.iu.dsc.tws.comms.dfw.io.Tuple;

/**
 * Created by pulasthi on 9/20/18.
 */
public class KReduceBatchFinalReceiver extends KReduceBatchReceiver {
  private static final Logger LOG = Logger.getLogger(KReduceBatchFinalReceiver.class.getName());

  /**
   * Final receiver that get the reduced values for the operation
   */
  private BulkReceiver bulkReceiver;

  public KReduceBatchFinalReceiver(ReduceFunction reduce, BulkReceiver receiver) {
    this.reduceFunction = reduce;
    this.bulkReceiver = receiver;
    this.limitPerKey = 1;
    this.isFinalBatchReceiver = true;
  }

  @Override
  @SuppressWarnings({"unchecked", "rawtypes"})
  public boolean progress() {
    boolean needsFurtherProgress = false;
    boolean sourcesFinished = false;
    for (int target : messages.keySet()) {
      if (batchDone.get(target)) {
        continue;
      }

      sourcesFinished = isSourcesFinished(target);
      if (!sourcesFinished && !(dataFlowOperation.isDelegateComplete()
          && messages.get(target).isEmpty())) {
        needsFurtherProgress = true;
      }

      if (sourcesFinished && dataFlowOperation.isDelegateComplete()) {
        batchDone.put(target, true);
        //TODO: check if we can simply remove the data, that is use messages.remove()
        bulkReceiver.receive(target, new ReduceIterator(messages.get(target)));
      }
    }

    return needsFurtherProgress;
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  private class ReduceIterator implements Iterator<Object> {

    private Map<Object, Queue<Object>> messageMap;
    private Queue<Object> keyList = new LinkedList<>();

    ReduceIterator(Map<Object, Queue<Object>> messageMap) {
      this.messageMap = messageMap;
      keyList.addAll(messageMap.keySet());
    }

    @Override
    public boolean hasNext() {
      return !keyList.isEmpty();
    }

    @Override
    public Tuple next() {
      Object key = keyList.poll();
      Queue<Object> value = messageMap.remove(key);
      return new Tuple(key, value.poll(), dataFlowOperation.getKeyType(),
          dataFlowOperation.getDataType());
    }
  }
}
